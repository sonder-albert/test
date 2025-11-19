package queue;

import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


/**
 * @author sonder
 */
public class MPMCLockFreeUnboundedQueue<E> {
    //MEMORY ACCESS
    private static final VarHandle OBJECT_VARHANDLE;

    static {
        try {
            OBJECT_VARHANDLE = MethodHandles.lookup().findVarHandle(PaddedObject.class, "value", Object.class);
        } catch (IllegalArgumentException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    //SEGMENT
    static final int STRIDE = 4;
    static final int SEGMENT_CAPACITY = 1024;
    static final int SEGMENT_MASK = SEGMENT_CAPACITY - 1;

    @Contended
    static final class PaddedLong {
        long value;

        public PaddedLong(long value) {
            this.value = value;
        }
    }

    @Contended
    static final class PaddedObject {
        Object value;

        public PaddedObject(Object value) {
            this.value = value;
        }
    }

    static final class Segment<E> {
        //actual storage (accessed via ARRAY_ELEM)
        final PaddedObject[] items;
        // per-slot sequence numbers (accessed via LONG_ARRAY_ELEM)
        final long[] sequences;
        final AtomicReference<Segment<E>> next = new AtomicReference<>(null);
        //该 segment 的 0 槽对应的全局 seq
        volatile long baseSeq;
        final int capacity;
        volatile long epoch;

        Segment(long baseSeq, long epoch) {
            this.capacity = SEGMENT_CAPACITY;
            this.items = new PaddedObject[capacity];
            this.sequences = new long[capacity * STRIDE];
            // 初始化 sequences：slot i 对应的 index = i*stride
            for (int i = 0; i < capacity; i++) {
                VarHandle.releaseFence();
                this.sequences[i * STRIDE] = baseSeq + i;
            }
        }

        public boolean ownsSeq(long seq) {
            return seq >= baseSeq && seq < baseSeq + capacity;
        }

        public void lazyReset(long newBase, long seq) {
            this.baseSeq = newBase;
            this.epoch = seq;
            this.next.setRelease(null);
            int slot = (int) (newBase & SEGMENT_MASK);
            this.sequences[slot * STRIDE] = newBase;
            this.items[slot].value = null;
        }

    }

    static final class MPSCRingBuffer<E> {
        private final E[] buffer;
        private final int mask;
        private volatile long producerIndex = 0;
        private volatile long consumerIndex = 0;

        private static final VarHandle P_INDEX;
        private static final VarHandle C_INDEX;
        private static final VarHandle ARRAY_ELEM;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                P_INDEX = l.findVarHandle(MPSCRingBuffer.class, "producerIndex", long.class);
                C_INDEX = l.findVarHandle(MPSCRingBuffer.class, "consumerIndex", long.class);
                ARRAY_ELEM = MethodHandles.arrayElementVarHandle(Object[].class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public MPSCRingBuffer(int capacity) {
            buffer = (E[]) new Object[capacity];
            mask = capacity - 1;
        }

        E poll() {
            long p = (long) P_INDEX.getOpaque(this);
            long c = (long) C_INDEX.getOpaque(this);
            if (c >= p) {
                return null;
            }
            int slot = (int) (p & mask);
            @SuppressWarnings("unchecked")
            E e = (E) ARRAY_ELEM.getAcquire(buffer, slot);
            if (e == null) {
                return null;
            }
            ARRAY_ELEM.setRelease(buffer, slot, null);
            C_INDEX.setRelease(this, c + 1);
            return e;
        }

        public boolean offer(E e) {
            while (true) {
                long p = (long) P_INDEX.getOpaque(this);
                long c = (long) C_INDEX.getOpaque(this);
                if (p - c >= buffer.length) {
                    return false;
                }
                if (P_INDEX.compareAndSet(this, p, p + 1)) {
                    int slot = (int) (p & mask);
                    ARRAY_ELEM.setRelease(buffer, slot, e);
                    return true;
                }
                Thread.onSpinWait();
            }
        }
    }

    //EPOCH
    //global epoch field
    @Contended
    static final class PaddedEpoch {
        final AtomicLong value = new AtomicLong(0);
    }

    final PaddedEpoch GLOBAL_EPOCH = new PaddedEpoch();
    private final int RUNTIME_PROCESSOR_COUNT;
    private final PaddedLong[] GLOBAL_EPOCH_TABLE;

    private int getThreadSlot() {
        int id = System.identityHashCode(Thread.currentThread());
        int slot = (id & (RUNTIME_PROCESSOR_COUNT - 1));
        return slot;
    }

    private final int LocalBufferHolder_BATCH_SIZE;

    static boolean bLessThanA(long a, long b) {
        return (a - b) < 0;
    }

    //global min value smaller than real is ok
    long getGlobalMinEpochApproch() {
        long min = Long.MAX_VALUE;
        for (PaddedLong e : GLOBAL_EPOCH_TABLE) {
            long value = e.value;
            if (bLessThanA(value, min)) {
                min = value;
            }
        }
        return min;
    }

    //todo reconstruct freelist data sturcture
    private final MPSCRingBuffer<Segment<E>> freeList;
    private final MPSCRingBuffer<Segment<E>> retiredList;

    //consumer reclaim control
    //appromix op number is ok
    private long globalOpCounterLazy = 0L;
    private final long reclaimThresholdMask;
    private final int maxReclaimPerRun;

    //local retired buffer
    private static final int LOCAL_RETIRED_CAPACITY = 8;
    private final ScopedValue<LocalBufferHolder<E>> LOCAL_BUFFER;

    //queue filed
    //todo how to reduce head and tail cas frequency
    final AtomicReference<Segment<E>> head;
    final AtomicReference<Segment<E>> tail;

    static final class LocalBufferHolder<E> {
        Segment<E> head;
        Segment<E> tail;

        final Segment<E>[] retiredList;
        final Segment<E>[] freshMono;
        int size;

        long epoch;
        long base;
        long limit;

        @SuppressWarnings("unchecked")
        LocalBufferHolder(int capacity, Segment<E> head, Segment<E> tail) {
            retiredList = new Segment[capacity];
            freshMono = new Segment[1];
            this.head = head;
            this.tail = tail;
        }

        void addRetired(Segment<E> segment, long localEpoch) {
            segment.epoch = localEpoch;
            retiredList[size++] = segment;
        }

        void addMono(Segment<E> segment) {
            freshMono[0] = segment;
        }

        Segment<E> getMono() {
            Segment<E> value = freshMono[0];
            freshMono[0] = null;
            return value;
        }

        Segment<E> peekMono() {
            return freshMono[0];
        }

        boolean isFull() {
            return size >= retiredList.length;
        }
    }

    private void retireSegment(Segment<E> seg) {
        // 优先放到本地 retired 缓冲，批量 flush 到全局
        addRetiredToLocal(seg);
    }

    // 将本地 retired 缓冲 flush 到全局 retiredPool
    private void flushLocalRetired() {
        LocalBufferHolder<E> buf = LOCAL_BUFFER.get();
        int c = buf.size;
        if (c == 0) {
            return;
        }
        for (int i = 0; i < c; i++) {
            Segment<E> re = buf.retiredList[i];
            retiredList.offer(re);
            buf.retiredList[i] = null;
        }
        buf.size = 0;
    }

    private void addRetiredToLocal(Segment<E> s) {
        LocalBufferHolder<E> buf = LOCAL_BUFFER.get();
        long epoch = buf.epoch;
        buf.addRetired(s, epoch);
        if (buf.isFull()) {
            flushLocalRetired();
        }
    }

    private void tryReclaim() {
        long op = globalOpCounterLazy++;
        if ((op & reclaimThresholdMask) != 0) {
            return; // not thredshold yet
        }
        reclaimBatch();
    }

    private void reclaimBatch() {
        long minInuse = getGlobalMinEpochApproch();
        int reclaimed = 0;
        Segment<E> rs;
        while (reclaimed < maxReclaimPerRun
                && (rs = retiredList.poll()) != null
                && rs.epoch < minInuse) {
            rs.baseSeq = 0;
            rs.next.setPlain(null);
            LocalBufferHolder<E> buf = LOCAL_BUFFER.get();
            //case 1 add local mono
            if (buf.peekMono() == null) {
                buf.addMono(rs);
            } else {
                //case 2 add global free list
                freeList.offer(rs);
            }
            reclaimed++;
        }
    }

    public MPMCLockFreeUnboundedQueue() {
        RUNTIME_PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();
        GLOBAL_EPOCH_TABLE = new PaddedLong[RUNTIME_PROCESSOR_COUNT];
        Arrays.fill(GLOBAL_EPOCH_TABLE, new PaddedLong(Long.MAX_VALUE));
        LOCAL_BUFFER = ScopedValue.newInstance();
        freeList = new MPSCRingBuffer<>(1024);
        retiredList = new MPSCRingBuffer<>(1024);
        Segment<E> first = new Segment<>(SEGMENT_CAPACITY, 0);
        head = new AtomicReference<>(first);
        tail = new AtomicReference<>(first);


        long reclaimThreshold = 1024;
        this.reclaimThresholdMask = reclaimThreshold - 1;
        this.LocalBufferHolder_BATCH_SIZE = 64;
        this.maxReclaimPerRun = 32;

        //warm up
        for (int i = 0; i < 16; i++) {
            freeList.offer(new Segment<>(SEGMENT_CAPACITY, 0));
        }
    }

    private static void backOff(int count) {
        if (count < 3) {
            //fast path spin
            Thread.onSpinWait();
            return;
        }
        //exponential backoff
        int shift = Math.min(count, 20);
        long delay = (1L << Math.min(count,20)) + ThreadLocalRandom.current().nextLong(50);
        LockSupport.parkNanos(delay);
    }

    private Segment<E> locateSegmentForPut(int count) {
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        int readCount = 0;
        while (true) {
            Segment<E> curTail = local.tail;
            long seq = local.epoch;
            long limit = local.limit;

            if (curTail.ownsSeq(seq)) {
                return curTail;
            } else {
                if (readCount++ > 3) {
                    curTail = this.tail.getAcquire();
                    local.tail = curTail;
                    local.head = this.head.getAcquire();
                }
            }

            if (seq >= limit) {
                Segment<E> next = curTail.next.getAcquire();
                if (next != null) {
                    this.tail.compareAndSet(curTail, next);
                    continue;
                }
                LocalBufferHolder<E> buffer = LOCAL_BUFFER.get();
                long newBase  = ((seq / SEGMENT_CAPACITY) * SEGMENT_CAPACITY);
                //case1 get thread local mono
                Segment<E> newTail = buffer.getMono();
                if (newTail == null) {
                    //case2 get global free list
                    newTail = freeList.poll();
                }
                if (newTail != null) {
                    //lazy reset
                    newTail.lazyReset(newBase, seq);
                }
                //case3 construct new segment
                if (newTail == null) {
                    newTail = new Segment<>(newBase, seq);
                }
                if (curTail.next.compareAndSet(null, newTail)) {
                    this.tail.compareAndSet(curTail, newTail);
                    return newTail;
                } else {
                    backOff(++count);
                }
            } else {
                backOff(++count);
            }
        }
    }


    private Segment<E> locateSegmentForTake(int count) {
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        while (true) {
            Segment<E> currentHead = local.head;
            long seq = local.epoch;
            long limit = local.limit;
            if (currentHead.ownsSeq(seq)) {
                return currentHead;
            } else {
                currentHead = this.head.getAcquire();
                local.head = currentHead;
                local.tail = this.tail.getAcquire();
            }
            if (seq >= limit) {
                Segment<E> next = currentHead.next.getAcquire();
                if (next == null) {
                    return null;
                }
                if (this.head.compareAndSet(currentHead, next)) {
                    retireSegment(currentHead);
                } else {
                    backOff(++count);
                }
            } else {
                backOff(++count);
            }
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public void offer(E value) {
        Objects.requireNonNull(value);
        LocalBufferHolder<E> buf = LOCAL_BUFFER.orElse(null);
        try {
            if (buf == null) {
                buf = new LocalBufferHolder<>(LOCAL_RETIRED_CAPACITY, head.getAcquire(), tail.getAcquire());
                ScopedValue.where(LOCAL_BUFFER, buf)
                        .run(() -> doPut(value));
            } else {
                doPut(value);
            }
        } finally {
            // batched reclaim trigger
            long ops = globalOpCounterLazy++;
            if ((ops & reclaimThresholdMask) == 0) {
                tryReclaim();
            }
        }
    }

    private void doPut(E value) {
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        int count = 0;
        while (true) {
            if (local.base >= local.limit) {
                //batch get seq
                long newBase = GLOBAL_EPOCH.value.getAndAdd(LocalBufferHolder_BATCH_SIZE);
                local.base = newBase;
                local.limit = newBase + LocalBufferHolder_BATCH_SIZE;
            }

            long seq = local.base++;
            local.epoch = seq;
            //update global epoch table
            int threadSlot = getThreadSlot();
            GLOBAL_EPOCH_TABLE[threadSlot].value = seq;
            int finalCount = count;
            Segment<E> newTail = locateSegmentForPut(finalCount);

            int slot = (int) (seq & SEGMENT_MASK);
            long observed = newTail.sequences[slot * STRIDE];
            if (seq == observed) {
                OBJECT_VARHANDLE.setRelease(newTail.items[slot], value);
                VarHandle.releaseFence();
                newTail.sequences[slot * STRIDE] = seq + 1L;
                return;
            }
            if (observed < newTail.baseSeq) {
                //lazy clean,retry write
                newTail.items[slot].value = null;
                newTail.sequences[slot * STRIDE] = seq;
                continue;
            }
            backOff(++count);
        }
    }


    public E poll() {
        LocalBufferHolder<E> buf = LOCAL_BUFFER.orElse(null);
        try {
            if (buf == null) {
                buf = new LocalBufferHolder<>(LOCAL_RETIRED_CAPACITY, head.getAcquire(), tail.getAcquire());
                return ScopedValue.where(LOCAL_BUFFER, buf)
                        .call(this::doTake);
            } else {
                return doTake();
            }
        } finally {
            long ops = globalOpCounterLazy++;
            if ((ops & reclaimThresholdMask) == 0) {
                tryReclaim();
            }
        }
    }

    private E doTake() {
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        int count = 0;
        while (true) {
            if (local.base >= local.limit) {
                //batch get seq
                long newBase = GLOBAL_EPOCH.value.getAndAdd(LocalBufferHolder_BATCH_SIZE);
                local.base = newBase;
                local.limit = newBase + LocalBufferHolder_BATCH_SIZE;
            }

            long seq = local.base++;
            local.epoch = seq;
            //update global epoch table
            int threadSlot = getThreadSlot();
            GLOBAL_EPOCH_TABLE[threadSlot].value = seq;
            int finalCount = count;
            Segment<E> currentHead = locateSegmentForTake(finalCount);
            Segment<E> localHead = local.head;
            Segment<E> localTail = local.tail;
            //double check,fast return null
            if (currentHead == null) {
                if (localHead == localTail) {
                    return null; // Queue is truly empty
                }
                //consumer spin waiting producer
                backOff(count++);
                continue;
            }

            int slot = (int) (seq & SEGMENT_MASK);
            long observe = currentHead.sequences[slot * STRIDE];
            long expected = seq + 1L;
            if (observe == expected) {
                PaddedObject cell = currentHead.items[slot];
                @SuppressWarnings("unchecked")
                E value = (E) OBJECT_VARHANDLE.getAcquire(cell);
                OBJECT_VARHANDLE.setRelease(cell, null);
                VarHandle.releaseFence();
                currentHead.sequences[slot * STRIDE] = seq + SEGMENT_CAPACITY;
                return value;
            } else if (observe < currentHead.baseSeq) {
                //stale slot.skip
                return null;
            } else {
                backOff(count++);
            }
        }
    }

}

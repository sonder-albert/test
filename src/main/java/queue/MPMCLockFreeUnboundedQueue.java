package queue;

import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


/**
 * @author sonder
 */
public class MPMCLockFreeUnboundedQueue<E> {
    //SEGMENT
    static final int STRIDE = 8;
    static final int SEGMENT_CAPACITY = 1024;
    static final int SEGMENT_MASK = SEGMENT_CAPACITY - 1;

    private static final VarHandle LONG_ARRAY_HANDLE;
    private static final VarHandle OBJECT_ARRAY_HANDLE;

    static {
        try {
            LONG_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(long[].class);
            OBJECT_ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException(e);
        }
    }

    @Contended
    static final class Segment<E> {
        //actual storage
        final E[] items;
        // per-slot sequence numbers,liner based
        final long[] sequences;
        final AtomicReference<Segment<E>> next = new AtomicReference<>(null);
        //该 segment 的 0 槽对应的全局 seq
        volatile long baseSeq;
        final int capacity;
        volatile long epoch;

        @SuppressWarnings("unchecked")
        Segment(long baseSeq) {
            this.baseSeq = baseSeq;
            this.capacity = SEGMENT_CAPACITY;
            int paddedLength = capacity * STRIDE;
            this.items = (E[]) new Object[paddedLength];
            this.sequences = new long[paddedLength];
            // 初始化 sequences：slot i 对应的 index = i*stride
            for (int i = 0; i < capacity; i++) {
                this.sequences[i * STRIDE] = baseSeq + i;
            }
            VarHandle.releaseFence();
        }

        public boolean ownsSeq(long seq) {
            return seq >= baseSeq && seq < baseSeq + capacity;
        }

        public void lazyReset(long newBase) {
            this.baseSeq = newBase;
            this.next.setRelease(null);
            int slot = (int) (newBase & SEGMENT_MASK);
            this.sequences[slot * STRIDE] = newBase;
            this.items[slot] = null;
            VarHandle.releaseFence();
        }

    }

    static final class MPSCRingBuffer<E> {
        private final Object[] buffer;
        private final int mask;
        private long producerIndex = 0;
        private long consumerIndex = 0;

        private static final VarHandle P_INDEX;
        private static final VarHandle C_INDEX;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                P_INDEX = l.findVarHandle(MPSCRingBuffer.class, "producerIndex", long.class);
                C_INDEX = l.findVarHandle(MPSCRingBuffer.class, "consumerIndex", long.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public MPSCRingBuffer(int capacity) {
            buffer = new Segment[capacity];
            mask = capacity - 1;
        }

        E poll(long seq) {
            long p = (long) P_INDEX.getOpaque(this);
            long c = (long) C_INDEX.getOpaque(this);
            if (c >= p) {
                return null;
            }
            int slot = (int) (p & mask);
            VarHandle.acquireFence();
            Segment<E> e = (Segment<E>) buffer[slot];
            if (e == null) {
                return null;
            }
            e.lazyReset(seq);
            buffer[slot] = null;
            VarHandle.releaseFence();
            C_INDEX.setRelease(this, c + 1);
            return (E) e;
        }

        E peek() {
            long p = (long) P_INDEX.getOpaque(this);
            long c = (long) C_INDEX.getOpaque(this);
            if (c >= p) {
                return null;
            }
            int slot = (int) (p & mask);
            VarHandle.acquireFence();
            Segment<E> e = (Segment<E>) buffer[slot];
            if (e == null) {
                return null;
            }
            return (E) e;
        }

        public void offer(E e) {
            while (true) {
                long p = (long) P_INDEX.getOpaque(this);
                long c = (long) C_INDEX.getOpaque(this);
                if (p - c >= buffer.length) {
                    return;
                }
                if (P_INDEX.compareAndSet(this, p, p + 1)) {
                    int slot = (int) (p & mask);
                    VarHandle.releaseFence();
                    buffer[slot] = e;
                    return;
                }
                Thread.onSpinWait();
            }
        }
    }

    //EPOCH
    @Contended
    public final AtomicLong PRODUCER_PRE_TOUCH_EPOCH = new AtomicLong(0);

    private final int LOCAL_PRODUCER_PRE_TOUCH_SIZE;
    @Contended
    final AtomicLong CONSUMER_PRE_TOUCH_EPOCH = new AtomicLong(0);

    private final int RUNTIME_PROCESSOR_COUNT;
    //Epoch Based Reclamation
    @Contended
    private final long[] CONSUMER_EPOCH_TABLE;
    //flow control
    @Contended
    private final long[] PRODUCER_EPOCH_TABLE;

    //global min value smaller than real min is ok,will not recliam using segment
    static long getGlobalMinUsingEpoch(long[] consumer, long[] producer) {
        long min = Long.MAX_VALUE;
        for (int cpu = 0; cpu < consumer.length; cpu += STRIDE) {
            long e = (long) LONG_ARRAY_HANDLE.getAcquire(consumer, cpu);
            if (e != 0 && e < min) {
                min = e;
            }
        }
        for (int cpu = 0; cpu < producer.length; cpu += STRIDE) {
            long e = (long) LONG_ARRAY_HANDLE.getAcquire(producer, cpu);
            if (e != 0 && e < min) {
                min = e;
            }
        }
        return min;
    }

    long updatePublishEpoch() {
        long max = Long.MIN_VALUE;
        for (int cpu = 0; cpu < PRODUCER_EPOCH_TABLE.length; cpu += STRIDE) {
            long e = (long) LONG_ARRAY_HANDLE.getAcquire(PRODUCER_EPOCH_TABLE, cpu);
            if (e != 0 && e > max) {
                max = e;
            }
        }
        return Math.clamp(max, 0, Long.MAX_VALUE);
    }

    private void updateProducerEpochMonotonic(int index, long newSeq) {
        // 循环 CAS，确保只有当新值大于旧值时才更新
        long current = (long) LONG_ARRAY_HANDLE.getAcquire(PRODUCER_EPOCH_TABLE, index);
        while (newSeq > current) {
            if (LONG_ARRAY_HANDLE.compareAndSet(PRODUCER_EPOCH_TABLE, index, current, newSeq)) {
                return;
            }
            current = (long) LONG_ARRAY_HANDLE.getAcquire(PRODUCER_EPOCH_TABLE, index);
        }
    }

    @Contended
    private final MPSCRingBuffer<Segment<E>> freeList;
    @Contended
    private final MPSCRingBuffer<Segment<E>> retiredList;

    //reclaim control
    //appromix op number is ok
    private long globalOpCounterLazy = 0L;
    private final long reclaimThresholdMask;
    private static final int MAX_RECLAIM_PER_RUN = 32;

    //local retired buffer
    private static final int LOCAL_RETIRED_CAPACITY = 8;
    private final ThreadLocal<LocalBufferHolder<E>> LOCAL_BUFFER;

    private final Cleaner CLEANER = Cleaner.create();

    //queue filed
    @Contended
    final AtomicReference<Segment<E>> head;
    @Contended
    final AtomicReference<Segment<E>> tail;

    static final class LocalBufferHolder<E> implements Runnable {
        final int threadSlot;

        Segment<E> localHead;
        Segment<E> localTail;

        final Segment<E>[] retiredList;
        int size;
        final Segment<E>[] freshMono;

        long localConsumerCursor;
        long takeLimit;
        //local producer cursor
        long localPublish;
        long putLimit;

        final long[] CONSUMER_TABLE;
        final long[] PRODUCER_TABLE;
        final MPSCRingBuffer<Segment<E>> globalRetiredList;

        @SuppressWarnings("unchecked")
        LocalBufferHolder(int capacity, Segment<E> head, Segment<E> tail, int cpus,
                          long[] consumerTable, long[] producerTable,
                          MPSCRingBuffer<Segment<E>> globalRetiredList) {
            retiredList = new Segment[capacity];
            freshMono = new Segment[1];
            this.localHead = head;
            this.localTail = tail;
            int localThreadId = System.identityHashCode(Thread.currentThread());
            this.threadSlot = (localThreadId & Integer.MAX_VALUE) % cpus * STRIDE;
            this.CONSUMER_TABLE = consumerTable;
            this.PRODUCER_TABLE = producerTable;
            this.globalRetiredList = globalRetiredList;
        }

        void addRetired(Segment<E> segment) {
            retiredList[size++] = segment;
        }

        void addMono(Segment<E> segment) {
            freshMono[0] = segment;
        }

        Segment<E> getMono(long newBase) {
            Segment<E> value = freshMono[0];
            if (value == null) {
                return null;
            }
            value.lazyReset(newBase);
            freshMono[0] = null;
            return value;
        }

        Segment<E> peekMono() {
            return freshMono[0];
        }

        void removeMono() {
            if (freshMono == null) {
                return;
            }
            freshMono[0].next.setPlain(null);
            freshMono[0] = null;
        }

        boolean isFull() {
            return size >= retiredList.length;
        }

        @Override
        public void run() {
            // 线程退出时，将自己在消费者表中的记录设为 MAX，避免阻碍回收
            LONG_ARRAY_HANDLE.setRelease(CONSUMER_TABLE, threadSlot, Long.MAX_VALUE);
            //reclaim to global
            long minInuse = getGlobalMinUsingEpoch(CONSUMER_TABLE, PRODUCER_TABLE);
            for (int i = size; i >= 0; i--) {
                Segment<E> seg = retiredList[i];
                if (seg != null && seg.epoch < minInuse) {
                    retiredList[i] = null;
                    seg.next.setPlain(null);
                    globalRetiredList.offer(seg);
                }
            }
            Segment<E> mono = peekMono();
            if (mono != null && mono.epoch < minInuse) {
                removeMono();
                globalRetiredList.offer(mono);
            }
            VarHandle.releaseFence();
            //clear all didnt reclaim local segment again
            for (int i = 0; i < retiredList.length; i++) {
                Segment<E> seg = retiredList[i];
                if (seg != null) {
                    seg.next.setPlain(null);
                }
                retiredList[i] = null;
            }
            size = 0;
            Segment<E> seg = freshMono[0];
            if (seg != null) {
                seg.next.setPlain(null);
            }
            freshMono[0] = null;
        }
    }

    private void retireSegment(Segment<E> seg, LocalBufferHolder<E> buf) {
        // 优先放到本地 retired 缓冲，批量 flush 到全局
        addRetiredToLocal(seg, buf);
    }

    // 将本地 retired 缓冲 flush 到全局 retiredPool
    private void flushLocalRetired(LocalBufferHolder<E> buf) {
        int c = buf.size;
        if (c == 0) return;

        for (int i = 0; i < c; i++) {
            Segment<E> re = buf.retiredList[i];
            retiredList.offer(re);
            buf.retiredList[i] = null;
        }
        buf.size = 0;
        VarHandle.releaseFence();
    }

    private void addRetiredToLocal(Segment<E> s, LocalBufferHolder<E> buf) {
        if (buf.isFull()) {
            flushLocalRetired(buf);
        } else {
            buf.addRetired(s);
        }
    }

    private void tryReclaim() {
        long op = globalOpCounterLazy++;
        // not thredshold yet
        if ((op & reclaimThresholdMask) != 0) return;
        reclaimBatch();
    }

    private void reclaimBatch() {
        LocalBufferHolder<E> buf = LOCAL_BUFFER.get();
        long minInuse = getGlobalMinUsingEpoch(CONSUMER_EPOCH_TABLE, PRODUCER_EPOCH_TABLE);
        int reclaimed = 0;
        Segment<E> rs;
        while (reclaimed < MAX_RECLAIM_PER_RUN
                && (rs = retiredList.peek()) != null) {
            // 标准 EBR 检查：只有当 segment 的 epoch 小于全局最小 active epoch 时才回收
            // 还没到安全回收的时候,retiredList 应该是有序的，如果头都不能回收，后面的也不能
            if (rs.epoch >= minInuse) break;

            rs = retiredList.poll(buf.localPublish + 1);
            // 还没到安全回收的时候,retiredList 应该是有序的，如果头都不能回收，后面的也不能
            if (rs == null) break;

            rs.next.setPlain(null);
            //case 1 add local mono
            if (buf.peekMono() == null) {
                buf.addMono(rs);
            } else {
                //case 2 add global free list
                freeList.offer(rs);
            }
            reclaimed++;
        }
        VarHandle.releaseFence();
    }

    public MPMCLockFreeUnboundedQueue() {
        RUNTIME_PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();
        CONSUMER_EPOCH_TABLE = new long[RUNTIME_PROCESSOR_COUNT * STRIDE];
        PRODUCER_EPOCH_TABLE = new long[RUNTIME_PROCESSOR_COUNT * STRIDE];

        Segment<E> first = new Segment<>(0);
        head = new AtomicReference<>(first);
        tail = new AtomicReference<>(first);
        freeList = new MPSCRingBuffer<>(1024);
        retiredList = new MPSCRingBuffer<>(1024);
        LOCAL_BUFFER = ThreadLocal.withInitial(() -> {
            LocalBufferHolder<E> holder = new LocalBufferHolder<>(
                    LOCAL_RETIRED_CAPACITY,
                    head.getAcquire(),
                    tail.getAcquire(),
                    RUNTIME_PROCESSOR_COUNT,
                    CONSUMER_EPOCH_TABLE,
                    PRODUCER_EPOCH_TABLE,
                    retiredList
            );
            CLEANER.register(Thread.currentThread(), holder);
            return holder;
        });


        long reclaimThreshold = 2048;
        this.reclaimThresholdMask = reclaimThreshold - 1;
        this.LOCAL_PRODUCER_PRE_TOUCH_SIZE = 64;
    }

    private static void backOff(int count) {
        if (count < 3) {
            //fast path spin
            Thread.onSpinWait();
            return;
        }
        //exponential backoff
        int shift = Math.min(count, 20);
        long delay = (1L << shift) + ThreadLocalRandom.current().nextLong(50);
        LockSupport.parkNanos(delay);
    }

    private Segment<E> locateSegmentForPut(int count, LocalBufferHolder<E> local) {
        while (true) {
            Segment<E> curTail = local.localTail;
            long seq = local.localPublish + 1;
            //check bound
            if (curTail.ownsSeq(seq)) return curTail;

            long newBase = curTail.baseSeq + SEGMENT_CAPACITY;
            //seq > newBase,need new segment
            if (seq >= newBase) {
                //case0 get next until real tail
                Segment<E> newTail = curTail.next.getAcquire();
                while (newTail != null) {
                    if (this.tail.compareAndSet(curTail, newTail)) {
                        local.localTail = newTail;
                        break;
                    }
                    newTail = curTail.next.getAcquire();
                }
                //case1 get thread local mono
                if (newTail == null) newTail = local.getMono(newBase);
                //case2 get global free list
                if (newTail == null) newTail = freeList.poll(newBase);
                //case3 construct new segment
                if (newTail == null) newTail = new Segment<>(newBase);
                //update tail
                if (curTail.next.compareAndSet(null, newTail)) {
                    this.tail.compareAndSet(curTail, newTail);
                    local.localTail = newTail;
                }
                //check bounded again
                if (curTail.ownsSeq(seq)) return curTail;
            }
            backOff(++count);
        }
    }

    private Segment<E> locateSegmentForTake(int count, LocalBufferHolder<E> local) {
        while (true) {
            Segment<E> currentHead = local.localHead;
            long seq = local.localConsumerCursor + 1;

            if (currentHead.ownsSeq(seq)) {
                return currentHead;
            }
            currentHead = this.head.getAcquire();
            local.localHead = currentHead;
            local.localTail = this.tail.getAcquire();
            if (currentHead.ownsSeq(seq)) {
                return currentHead;
            }
            //find next
            if (seq >= currentHead.baseSeq + SEGMENT_CAPACITY) {
                Segment<E> next = currentHead.next.getAcquire();
                if (next == null) {
                    return null;
                }
                if (this.head.compareAndSet(currentHead, next)) {
                    retireSegment(currentHead, local);
                }
                //access concurrent update local head
                local.localHead = next;
            }
            backOff(++count);
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public void offer(E value) {
        Objects.requireNonNull(value);
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        if (local.localPublish + 1 >= local.putLimit) {
            //batch incr local producer cursor
            long newBase = PRODUCER_PRE_TOUCH_EPOCH.getAndAdd(LOCAL_PRODUCER_PRE_TOUCH_SIZE);
            local.putLimit = newBase + LOCAL_PRODUCER_PRE_TOUCH_SIZE;
            local.localPublish = newBase - 1;
        }
        long seq = local.localPublish + 1;

        int count = 0;
        try {
            while (true) {
                Segment<E> newTail = locateSegmentForPut(count, local);

                int slot = (int) (seq & SEGMENT_MASK) * STRIDE;
                long observed = (long) LONG_ARRAY_HANDLE.getAcquire(newTail.sequences, slot);
                if (seq == observed) {
                    OBJECT_ARRAY_HANDLE.setOpaque(newTail.items, slot, value);
                    VarHandle.storeStoreFence();
                    LONG_ARRAY_HANDLE.setRelease(newTail.sequences, slot, seq + 1);
                    //update global epoch table,
                    // notify reclaim this seq is using
                    //notify consumer this seq putted
                    updateProducerEpochMonotonic(local.threadSlot, seq + 1);
                    local.localPublish = seq;
                    return;
                }
                backOff(++count);
            }
        } finally {
            // batched reclaim trigger
            long ops = globalOpCounterLazy;
            if ((ops & reclaimThresholdMask) == 0) {
                tryReclaim();
            }
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public E poll() {
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        int count = 0;
        try {
            //acquire batch consumer limit
            if (local.localConsumerCursor + 1 >= local.takeLimit) {
                while (true) {
                    long consumerEpoch = CONSUMER_PRE_TOUCH_EPOCH.getAcquire();
                    long producerEpoch = updatePublishEpoch();

                    if (producerEpoch == consumerEpoch) {
                        return null;
                    }
                    // 只能申请到生产者已经推进到的位置
                    long batch = Math.clamp(producerEpoch - consumerEpoch, 0, LOCAL_PRODUCER_PRE_TOUCH_SIZE);
                    //no element can take ,waiting producer
                    if (batch > 0) {
                        //try acquire consumer pre touch batch
                        if (CONSUMER_PRE_TOUCH_EPOCH.compareAndSet(consumerEpoch, consumerEpoch + batch)) {
                            local.localConsumerCursor = consumerEpoch - 1;
                            local.takeLimit = consumerEpoch + batch;
                            break;
                        }
                    }
                    backOff(++count);
                }
            }

            while (true) {
                long seq = local.localConsumerCursor + 1;
                Segment<E> currentHead = locateSegmentForTake(count, local);
                //consumer spin waiting producer link new head
                if (currentHead == null) {
                    backOff(++count);
                    continue;
                }

                int slot = (int) (seq & SEGMENT_MASK) * STRIDE;
                long observe = (long) LONG_ARRAY_HANDLE.getAcquire(currentHead.sequences, slot);

                if (observe == seq + 1L) {
                    @SuppressWarnings("unchecked")
                    E value = (E) OBJECT_ARRAY_HANDLE.getOpaque(currentHead.items, slot);
                    if (value != null &&
                            OBJECT_ARRAY_HANDLE.compareAndSet(currentHead.items, slot, value, null)) {
                        local.localConsumerCursor = seq;
                        LONG_ARRAY_HANDLE.setRelease(currentHead.sequences, slot, seq + SEGMENT_CAPACITY);
                        VarHandle.storeStoreFence();
                        LONG_ARRAY_HANDLE.setRelease(CONSUMER_EPOCH_TABLE, local.threadSlot, seq + 1);
                        return value;
                    }
                }
                //queue empty
                if (observe < seq && currentHead.next.getOpaque() == null) return null;
                //if race happen.spin
                backOff(++count);
            }
        } finally {
            long ops = globalOpCounterLazy++;
            if ((ops & reclaimThresholdMask) == 0) {
                tryReclaim();
            }
        }
    }

}


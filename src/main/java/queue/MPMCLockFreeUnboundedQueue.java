package queue;

import jdk.internal.vm.annotation.Contended;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.ref.Cleaner;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;


public class MPMCLockFreeUnboundedQueue<E> {
    //SEGMENT
    private static final int STRIDE = 8;
    private static final int SEGMENT_CAPACITY = 1024;
    private static final int SEGMENT_MASK = SEGMENT_CAPACITY - 1;

    private static final VarHandle LONG_ARRAY_HANDLE;
    private static final VarHandle OBJECT_ARRAY_HANDLE;

    //EPOCH
    @Contended
    private final AtomicLong PRODUCER_EPOCH = new AtomicLong(-1);
    @Contended
    private final AtomicLong CONSUMED = new AtomicLong(0);

    //RECLAIM
    @Contended
    private final MPMCRingBuffer<Segment<E>> freeList;
    @Contended
    private final MPMCRingBuffer<Segment<E>> retiredList;

    @Contended
    private final Segment<E> ALLOCATING = new Segment<>(Long.MIN_VALUE);
    //local retired buffer
    private static final int LOCAL_RETIRED_CAPACITY = 64;
    private final ThreadLocal<LocalBufferHolder<E>> LOCAL_BUFFER;
    private final Cleaner CLEANER = Cleaner.create();

    //queue filed
    @Contended
    final AtomicReference<Segment<E>> head;
    @Contended
    final AtomicReference<Segment<E>> tail;

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
        final AtomicInteger consumed = new AtomicInteger(0);

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

        public void reset(long newBase) {
            this.baseSeq = newBase;
            this.epoch = 0;
            consumed.set(0);
            VarHandle.releaseFence();
            this.next.setRelease(null);
            for (int i = 0; i < capacity; i++) {
                LONG_ARRAY_HANDLE.setRelease(sequences, i * STRIDE, baseSeq + i);
            }
            for (int i = 0; i < capacity; i++) {
                OBJECT_ARRAY_HANDLE.setRelease(items, i * STRIDE, null);
            }
        }
    }

    static final class MPMCRingBuffer<E> {
        private final Object[] buffer;
        private final int mask;
        private long producerIndex = 0;
        private long consumerIndex = 0;

        private static final VarHandle P_INDEX;
        private static final VarHandle C_INDEX;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                P_INDEX = l.findVarHandle(MPMCRingBuffer.class, "producerIndex", long.class);
                C_INDEX = l.findVarHandle(MPMCRingBuffer.class, "consumerIndex", long.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        public MPMCRingBuffer(int capacity) {
            buffer = new Segment[capacity];
            mask = capacity - 1;
        }

        E poll() {
            long p;
            long c;
            do {
                p = (long) P_INDEX.getAcquire(this);
                c = (long) C_INDEX.getAcquire(this);
                if (c >= p) {
                    return null;
                }
            } while (!C_INDEX.compareAndSet(this, c, c + 1));

            int slot = (int) (c & mask);
            @SuppressWarnings("unchecked")
            Segment<E> e = (Segment<E>) OBJECT_ARRAY_HANDLE.getAcquire(buffer, slot);
            if (e != null) {
                OBJECT_ARRAY_HANDLE.setRelease(buffer, slot, null);
                return (E) e;
            }
            return null;
        }

        E peek() {
            long p = (long) P_INDEX.getAcquire(this);
            long c = (long) C_INDEX.getAcquire(this);
            if (c >= p) {
                return null;
            }
            int slot = (int) (c & mask);
            @SuppressWarnings("unchecked")
            E e = (E) OBJECT_ARRAY_HANDLE.getAcquire(buffer, slot);
            return e;
        }

        public void offer(E e) {
            while (true) {
                long p = (long) P_INDEX.getAcquire(this);
                long c = (long) C_INDEX.getAcquire(this);
                if (p - c >= buffer.length) {
                    Thread.onSpinWait();
                    continue;
                }
                if (P_INDEX.compareAndSet(this, p, p + 1)) {
                    int slot = (int) (p & mask);
                    OBJECT_ARRAY_HANDLE.setRelease(buffer, slot, e);
                    return;
                }
            }
        }
    }

    static final class LocalBufferHolder<E> implements Runnable {
        Segment<E> localHead;
        Segment<E> localTail;

        final Segment<E>[] freshMono;
        final Segment<E>[] retiredList;
        int size;

        final MPMCRingBuffer<Segment<E>> globalRetiredList;

        @SuppressWarnings("unchecked")
        LocalBufferHolder(int capacity, Segment<E> head, Segment<E> tail,
                          MPMCRingBuffer<Segment<E>> globalRetiredList) {
            retiredList = new Segment[capacity];
            freshMono = new Segment[1];
            this.localHead = head;
            this.localTail = tail;
            this.globalRetiredList = globalRetiredList;
        }

        void addLocalRetired(Segment<E> segment) {
            retiredList[size++] = segment;
        }

        void addMono(Segment<E> segment) {
            freshMono[0] = segment;
        }

        Segment<E> getMono() {
            Segment<E> value = freshMono[0];
            if (value == null) {
                return null;
            }
            freshMono[0] = null;
            return value;
        }

        Segment<E> peekMono() {
            return freshMono[0];
        }

        void removeMono() {
            freshMono[0].next.setRelease(null);
            freshMono[0] = null;
        }

        @Override
        public void run() {
            // producer epoch clean
            for (int i = size; i >= 0; i--) {
                Segment<E> seg = retiredList[i];
                if (seg != null) {
                    if (seg.consumed.get() == SEGMENT_CAPACITY) {
                        globalRetiredList.offer(seg);
                    }
                    seg.next.setRelease(null);
                    retiredList[i] = null;
                }
            }
            Segment<E> mono = peekMono();
            if (mono != null && mono.consumed.get() == SEGMENT_CAPACITY) {
                removeMono();
                globalRetiredList.offer(mono);
            }
            size = 0;
            VarHandle.releaseFence();
        }
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

    private void retireSegment(Segment<E> s, LocalBufferHolder<E> buf) {
        buf.addLocalRetired(s);
        if (buf.size >= LOCAL_RETIRED_CAPACITY) {
            tryReclaim();
            flushLocalRetired(buf);
        }
    }

    private void tryReclaim() {
        LocalBufferHolder<E> buf = LOCAL_BUFFER.get();
        int reclaimed = 0;
        Segment<E> rs;
        while (reclaimed < LOCAL_RETIRED_CAPACITY
                && (rs = retiredList.peek()) != null) {
            // 标准 EBR 检查：只有当 segment 全部被消费完毕时才回收
            // 还没到安全回收的时候,retiredList 应该是有序的，如果头都不能回收，后面的也不能
            //check segment whether all consumed
            if (rs.consumed.getAcquire() != SEGMENT_CAPACITY) break;
            rs = retiredList.poll();
            // 还没到安全回收的时候,retiredList 应该是有序的，如果头都不能回收，后面的也不能
            if (rs == null) break;
            rs.next.setOpaque(null);
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
        Segment<E> first = new Segment<>(0);
        head = new AtomicReference<>(first);
        tail = new AtomicReference<>(first);
        freeList = new MPMCRingBuffer<>(1024);
        retiredList = new MPMCRingBuffer<>(1024);
        LOCAL_BUFFER = ThreadLocal.withInitial(() -> {
            LocalBufferHolder<E> holder = new LocalBufferHolder<>(
                    LOCAL_RETIRED_CAPACITY,
                    head.getAcquire(),
                    tail.getAcquire(),
                    retiredList
            );
            CLEANER.register(Thread.currentThread(), holder);
            return holder;
        });
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

    private Segment<E> locateSegmentForPut(long seq, int count, LocalBufferHolder<E> local) {
        while (true) {
            Segment<E> curTail = local.localTail;
            //check bound
            if (curTail.ownsSeq(seq)) return curTail;

            long newBase = curTail.baseSeq + SEGMENT_CAPACITY;
            //seq > newBase,link new segment
            if (seq >= newBase) {
                //check has next
                Segment<E> next = curTail.next.getAcquire();
                if (next != null) {
                    //wait : other thread constructing and linking
                    if (next == ALLOCATING) {
                        backOff(++count);
                        continue;
                    }
                    //
                    this.tail.compareAndSet(curTail, next);
                    local.localTail = next;
                    continue;
                }
                //fast path: reuse pooled segment
                //case1 get thread local mono
                Segment<E> pooled = local.getMono();
                //case2 get global free list
                if (pooled == null) pooled = freeList.poll();
                //try to link pooled
                //double check,next may constructed by other thread
                if (pooled != null) {
                    // 如果有缓存对象，直接 CAS 链接
                    if (curTail.next.compareAndSet(null, pooled)) {
                        pooled.reset(newBase);
                        this.tail.compareAndSet(curTail, pooled);
                        local.localTail = pooled;
                        continue;
                    }
                    // Step 4: CAS failed → another thread linked → update tailSeg -> seg offer back free list
                    Segment<E> realNext = curTail.next.getAcquire();
                    if (realNext != null && realNext != ALLOCATING) {
                        local.localTail = realNext;
                        this.tail.compareAndSet(curTail, realNext);
                    }
                    if (local.peekMono() == null) {
                        local.addMono(pooled);
                    } else {
                        freeList.offer(pooled);
                    }
                    continue;
                }
                //slow path: construct new segment
                if (curTail.next.compareAndSet(null, ALLOCATING)) {
                    try {
                        Segment<E> fresh = new Segment<>(newBase);

                        // 将占位符替换为真实对象 (setRelease 保证可见性)
                        curTail.next.setRelease(fresh);

                        // 顺便推进 tail
                        this.tail.compareAndSet(curTail, fresh);
                        local.localTail = fresh;
                    } catch (Throwable t) {
                        // 防御性编程：万一 OOM 了，把坑位还原，否则队列死锁
                        curTail.next.compareAndSet(ALLOCATING, null);
                    }
                }
            }
            backOff(++count);
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public void offer(E value) {
        Objects.requireNonNull(value);
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        long seq = PRODUCER_EPOCH.getAndIncrement() + 1;
        int count = 0;

        while (true) {

            Segment<E> tailSeg = locateSegmentForPut(seq, count, local);

            int slot = (int) (seq & SEGMENT_MASK) * STRIDE;
            long observed = (long) LONG_ARRAY_HANDLE.getAcquire(tailSeg.sequences, slot);
            if (seq == observed &&
                    OBJECT_ARRAY_HANDLE.compareAndSet(tailSeg.items, slot, null, value)) {
                LONG_ARRAY_HANDLE.setRelease(tailSeg.sequences, slot, seq + 1);
                return;
            }
            backOff(++count);
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public E poll() {
        LocalBufferHolder<E> local = LOCAL_BUFFER.get();
        int count = 0;

        while (true) {
            long seq = CONSUMED.getAcquire();
            Segment<E> headSeg = locateSegmentForTake(local, seq);
            int slot = (int) (seq & SEGMENT_MASK) * STRIDE;
            long observe = (long) LONG_ARRAY_HANDLE.getAcquire(headSeg.sequences, slot);

            if (observe == seq + 1) {
                @SuppressWarnings("unchecked")
                E value = (E) OBJECT_ARRAY_HANDLE.getAcquire(headSeg.items, slot);

                if (value != null && CONSUMED.compareAndSet(seq, seq + 1)) {
                    OBJECT_ARRAY_HANDLE.setRelease(headSeg.items, slot, null);
                    LONG_ARRAY_HANDLE.setRelease(headSeg.sequences, slot, seq + SEGMENT_CAPACITY);
                    headSeg.consumed.incrementAndGet();
                    return value;
                }
            } else if (observe > seq + 1) {
                count = 0;
            }
            //strict check empty
            if (count > 200 &&
                    CONSUMED.getAcquire() >= PRODUCER_EPOCH.getAcquire() &&
                    headSeg.next.getAcquire() == null) {
                return null;
            }
            //if race happen.spin
            backOff(++count);
        }
    }

    private Segment<E> locateSegmentForTake(LocalBufferHolder<E> local, long seq) {
        Segment<E> next;
        Segment<E> currentHead;
        int count = 0;
        while (true) {
            currentHead = local.localHead;

            if (currentHead.ownsSeq(seq)) {
                return currentHead;
            }

            //find next,promise current segment all slot consumed
            if (currentHead.consumed.getAcquire() == SEGMENT_CAPACITY) {
                next = currentHead.next.getAcquire();

                if (next != null && next != ALLOCATING) {
                    local.localHead = next;
                    if (this.head.compareAndSet(currentHead, next)) {
                        retireSegment(currentHead, local);
                    }
                }
            }
            //consumer spin waiting producer link new head
            backOff(++count);
        }
    }

}


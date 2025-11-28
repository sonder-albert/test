package queue;

import module java.base;

import jdk.internal.misc.Unsafe;
import jdk.internal.vm.annotation.Contended;
import jdk.internal.vm.annotation.ForceInline;

import java.util.Objects;

public class MPMCLockFreeUnboundedQueue<E> {
    //SEGMENT field
    private static final byte STRIDE_SHIFT = 3;
    private final int SEGMENT_CAPACITY;
    private final int SEGMENT_MASK;

    private final int SEG_SHIFT;
    private final int CPUS;

    // Unsafe & Offsets
    private static final Unsafe UNSAFE;
    private static final long LONG_ARRAY_BASE;
    private static final int LONG_ARRAY_SHIFT;
    private static final long OBJECT_ARRAY_BASE;
    private static final int OBJECT_ARRAY_SHIFT;
    private static final int SEQ_ENTRY_SHIFT;
    private static final int ITEM_ENTRY_SHIFT;

    private static final long PRODUCER;
    private static final long CONSUMER;
    private static final long HEAD;
    private static final long TAIL;
    private static final long SEGMENT_STATE;
    private static final long SEGMENT_NEXT;
    private static final long SEGMENT_PREV;
    private static final long SEGMENT_CONSUMED;
    private static final long SEGMENT_PRODUCED;

    @Contended("producer")
    private volatile long producer = 0;
    @Contended("producer")
    private volatile Segment<E> tail;
    @Contended("producer")
    private final ThreadLocal<Segment<E>> LAST_KNOWN_PRODUCER;
    @Contended("consumer")
    private volatile long consumer = 0;
    @Contended("consumer")
    private volatile Segment<E> head;
    @Contended("consumer")
    private final ThreadLocal<Segment<E>> LAST_KNOWN_CONSUMER;

    //init handles
    static {
        try {
            // 1. Setup Unsafe
            UNSAFE = Unsafe.getUnsafe();

            // 2. Setup Array Offsets
            LONG_ARRAY_BASE = UNSAFE.arrayBaseOffset(long[].class);
            int longScale = UNSAFE.arrayIndexScale(long[].class);
            if ((longScale & (longScale - 1)) != 0) {
                throw new Error("long[] data type scale not a power of two");
            }
            LONG_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(longScale);
            SEQ_ENTRY_SHIFT = LONG_ARRAY_SHIFT + STRIDE_SHIFT;

            OBJECT_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class);
            int objScale = UNSAFE.arrayIndexScale(Object[].class);
            if ((objScale & (objScale - 1)) != 0) {
                throw new Error("Object[] data type scale not a power of two");
            }
            OBJECT_ARRAY_SHIFT = 31 - Integer.numberOfLeadingZeros(objScale);
            ITEM_ENTRY_SHIFT = OBJECT_ARRAY_SHIFT + STRIDE_SHIFT;

            // 获取字段偏移量，消除 VarHandle 开销
            PRODUCER = UNSAFE.objectFieldOffset(MPMCLockFreeUnboundedQueue.class.getDeclaredField("producer"));
            CONSUMER = UNSAFE.objectFieldOffset(MPMCLockFreeUnboundedQueue.class.getDeclaredField("consumer"));
            HEAD = UNSAFE.objectFieldOffset(MPMCLockFreeUnboundedQueue.class.getDeclaredField("head"));
            TAIL = UNSAFE.objectFieldOffset(MPMCLockFreeUnboundedQueue.class.getDeclaredField("tail"));
            SEGMENT_STATE = UNSAFE.objectFieldOffset(Segment.class.getDeclaredField("state"));
            SEGMENT_NEXT = UNSAFE.objectFieldOffset(Segment.class.getDeclaredField("next"));
            SEGMENT_PREV = UNSAFE.objectFieldOffset(Segment.class.getDeclaredField("prev"));
            SEGMENT_CONSUMED = UNSAFE.objectFieldOffset(Segment.class.getDeclaredField("consumed"));
            SEGMENT_PRODUCED = UNSAFE.objectFieldOffset(Segment.class.getDeclaredField("produced"));
        } catch (IllegalArgumentException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public MPMCLockFreeUnboundedQueue(int capacity) {
        SEGMENT_CAPACITY = capacity;
        SEGMENT_MASK = capacity - 1;
        SEG_SHIFT = 31 - Integer.numberOfLeadingZeros(SEGMENT_CAPACITY);

        head = tail = new Segment<>(0, capacity, SEG_SHIFT);

        LAST_KNOWN_PRODUCER = ThreadLocal.withInitial(() -> (Segment<E>) UNSAFE.getReferenceAcquire(this, TAIL));
        LAST_KNOWN_CONSUMER = ThreadLocal.withInitial(() -> (Segment<E>) UNSAFE.getReferenceAcquire(this, HEAD));
        CPUS = Runtime.getRuntime().availableProcessors();
    }

    @Contended
    static final class Segment<E> {
        static final byte NOT_LINKED = 0;
        static final byte LINKED = 1;
        static final byte REMOVED = 2;

        volatile byte state = NOT_LINKED;
        volatile Segment<E> next;
        volatile Segment<E> prev;

        final long base;
        final long limit;

        final long index;
        @Contended("consumer")
        volatile int consumed;
        @Contended("producer")
        volatile int produced;
        //item and sequence index = global seq - base
        //actual storage
        volatile E[] items;
        // per-slot sequence numbers,liner based
        volatile long[] sequences;

        //初始化不需要加fence,因为segment创建状态只会被一个线程抢占
        @SuppressWarnings("unchecked")
        Segment(long base, int capacity, int shift) {
            this.base = base;
            this.limit = base + capacity;
            this.index = base >>> shift;
            int paddedLength = capacity << STRIDE_SHIFT;
            this.items = (E[]) new Object[paddedLength];
            this.sequences = new long[paddedLength];
            // 初始化 sequences：slot i 对应的 index = i*stride
            for (int i = 0; i < capacity; i++) {
                this.sequences[i << STRIDE_SHIFT] = base + i;
            }
        }

        //@ForceInline
        public boolean owns(long seq) {
            return seq >= base && seq < limit;
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public void offer(E value) {
        Objects.requireNonNull(value);

        long seq = UNSAFE.getAndAddLong(this, PRODUCER, 1);

        Segment<E> seg = locateSegment(seq, TAIL, true);

        // 2. Calculate byte offsets for Unsafe access
        long seqOffset = LONG_ARRAY_BASE + ((seq & SEGMENT_MASK) << SEQ_ENTRY_SHIFT);
        long itemOffset = OBJECT_ARRAY_BASE + ((seq & SEGMENT_MASK) << ITEM_ENTRY_SHIFT);
        //dont need volatile, because seq is xadd option and sequence always increment
        while (UNSAFE.getLongVolatile(seg.sequences, seqOffset) != seq) {
            Thread.onSpinWait();
        }
        UNSAFE.putReferenceRelease(seg.items, itemOffset, value);
        // 5. Update Sequence (Release Semantics)
        UNSAFE.putLongRelease(seg.sequences, seqOffset, seq + 1);
        int put = UNSAFE.getAndAddInt(seg, SEGMENT_PRODUCED, 1);

        //first producer publish
        if (put == 0) {
            casGlobal(seg, TAIL);
        }
        //last producer expand
        if ((seq & SEGMENT_MASK) == 0 || put == SEGMENT_MASK) {
            checkExpand(seg);
        }
    }

    //Vyukov-style: sequence-based，0 slot CAS
    public E poll() {
        long seq = UNSAFE.getAndAddLong(this, CONSUMER, 1);
        // 1. Calculate Offsets
        long seqOffset = LONG_ARRAY_BASE + ((seq & SEGMENT_MASK) << SEQ_ENTRY_SHIFT);
        long itemOffset = OBJECT_ARRAY_BASE + ((seq & SEGMENT_MASK) << ITEM_ENTRY_SHIFT);

        Segment<E> seg = locateSegment(seq, HEAD, false);
        while (UNSAFE.getLongAcquire(seg.sequences, seqOffset) != seq + 1) {
            Thread.onSpinWait();
        }
        @SuppressWarnings("unchecked")
        E e = (E) UNSAFE.getAndSetReferenceRelease(seg.items, itemOffset, null);
        int take = UNSAFE.getAndAddInt(seg, SEGMENT_CONSUMED, 1);

        //first consumer publish
        if (take == 0) {
            casGlobal(seg, HEAD);
        }
        //last consumer clean
        if (take == SEGMENT_MASK) {
            tryClean(seg);
        }
        return e;
    }

    // 辅助方法：CAS 更新global head\tail
    @SuppressWarnings("unchecked")
    @ForceInline
    private void casGlobal(Segment<E> s, long global) {
        Segment<E> cur;
        do {
            cur = (Segment<E>) UNSAFE.getReferenceVolatile(this, global);
            // 如果当前 HEAD 已经比 newHead 新（index 更大），则不需要更新，直接退出
            if (cur.index >= s.index) {
                return;
            }
            // CAS 失败则重试，确保不会发生 ABA 或回滚
        } while (!UNSAFE.compareAndSetReference(this, global, cur, s));
    }

    @SuppressWarnings("unchecked")
    @ForceInline
    private Segment<E> locateSegment(long seq, long global, boolean forPut) {
        ThreadLocal<Segment<E>> tl = forPut ? LAST_KNOWN_PRODUCER : LAST_KNOWN_CONSUMER;
        //fast path
        Segment<E> cur = (Segment<E>) UNSAFE.getReferenceAcquire(this, global);
        if (cur.owns(seq)) {
            if ((seq & 31) == 0) {
                Segment<E> last = tl.get();
                if (last != cur) {
                    tl.set(cur);
                }
            }
            return cur;
        }
        //local cache
        Segment<E> last = tl.get();
        if (last.owns(seq)) {
            return last;
        }
        long expected = seq >>> SEG_SHIFT;
        long index = cur.index;
        long diff = Math.abs(expected - index);
        //heuristic search
        if (diff > STRIDE_SHIFT) {
            if (Math.abs(expected - last.index) < diff) {
                cur = last;
            }
        }
        //cold path
        while (!cur.owns(seq)) {
            //slow path: seq larger than cur get next
            while (seq >= cur.limit) {
                Segment<E> next = (Segment<E>) UNSAFE.getReferenceAcquire(cur, SEGMENT_NEXT);
                if (next != null && UNSAFE.getByteAcquire(next, SEGMENT_STATE) != Segment.REMOVED) {
                    cur = next;
                } else {
                    if (forPut) {
                        //走到最新的地方了,真正需要参与扩容
                        if (cur.index == expected - 1) {
                            checkExpand(cur);
                        } else {
                            //next走到一半被clean方法清空了,需要交给外层循环处理
                            cur = (Segment<E>) UNSAFE.getReferenceAcquire(this, global);
                            break;
                        }
                    } else {
                        Thread.onSpinWait();
                    }
                }
            }
            //slow path: seq < seg base,from head find next
            while (seq < cur.base) {
                Segment<E> prev = (Segment<E>) UNSAFE.getReferenceAcquire(cur, SEGMENT_PREV);
                if (prev != null && UNSAFE.getByteAcquire(prev, SEGMENT_STATE) != Segment.REMOVED) {
                    cur = prev;
                } else {
                    //prev走到一半被clean方法清空了,需要交给外层循环处理
                    cur = (Segment<E>) UNSAFE.getReferenceAcquire(this, global);
                    break;
                }
            }
        }
        tl.set(cur);
        return cur;
    }

    @SuppressWarnings("unchecked")
    @ForceInline
    private void tryClean(Segment<E> cur) {
        Segment<E> p = cur.prev;
        //clean prev
        while (p != null) {
            Segment<E> prev = (Segment<E>) UNSAFE.getReferenceAcquire(p, SEGMENT_PREV);
            int consumed = UNSAFE.getIntAcquire(p, SEGMENT_CONSUMED);
            if (consumed != SEGMENT_CAPACITY) {
                break;
            }
            if (UNSAFE.compareAndSetByte(p, SEGMENT_STATE, Segment.LINKED, Segment.REMOVED)) {
                Segment<E> next = (Segment<E>) UNSAFE.getReferenceAcquire(p, SEGMENT_NEXT);
                //链接前后
                if (prev != null) {
                    UNSAFE.putReferenceVolatile(prev, SEGMENT_NEXT, next);
                }
                if (next != null) {
                    UNSAFE.putReferenceVolatile(next, SEGMENT_PREV, prev);
                }
                UNSAFE.putReferenceVolatile(p, SEGMENT_PREV, null);
                //UNSAFE.putReferenceVolatile(p, SEGMENT_NEXT, null);
                //释放大对象
                p.items = null;
                p.sequences = null;
                p = prev;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @ForceInline
    private void checkExpand(Segment<E> cur) {
        int failure = 0;
        while (true) {
            Segment<E> next = (Segment<E>) UNSAFE.getReferenceAcquire(cur, SEGMENT_NEXT);
            //truth publish
            if (next != null) {
                return;
            }
            byte state = UNSAFE.getByteAcquire(cur, SEGMENT_STATE);
            //waiting next publish
            if (state == Segment.LINKED) {
                Thread.onSpinWait();
                continue;
            }
            //spin first,if too long not ready,race create
            if (failure++ != 0 && failure < (CPUS >> 1)) {
                Thread.onSpinWait();
                continue;
            }
            //Speculative Creation
            Segment<E> fresh = new Segment<>(cur.limit, SEGMENT_CAPACITY, SEG_SHIFT);
            UNSAFE.putReferenceRelease(fresh, SEGMENT_PREV, cur);
            //Final Decision: CAS Next
            if (UNSAFE.compareAndSetReference(cur, SEGMENT_NEXT, null, fresh)) {
                //volatile publish
                UNSAFE.putByteVolatile(cur, SEGMENT_STATE, Segment.LINKED);
                return;
            } else {
                UNSAFE.putReferenceRelease(fresh, SEGMENT_PREV, null);
            }
        }
    }
}




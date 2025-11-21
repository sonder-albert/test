package unit;

import org.junit.jupiter.api.*;
import queue.MPMCLockFreeUnboundedQueue;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

class MPMCLockFreeUnboundedQueueTest {
    MPMCLockFreeUnboundedQueue<Long> q;
    // 样例：单生产者单消费者基本功能
    @Test
    void testBasicPutPollSingleProducerConsumer() {
        q=null;
        q = new MPMCLockFreeUnboundedQueue<>();
        // producer
        long v = System.nanoTime();
        q.offer(v);

        // consumer
        Long out = q.poll();
        assertNotNull(out, "应当有一个元素可取");
        assertEquals(v, out.longValue(), "取出值应等于放入值");
    }

    // 基本并发性：多生产者多消费者，确保没有丢失
    //@RepeatedTest(5)
    void testConcurrentPutPollManyProducersConsumers() throws InterruptedException {
        q=null;
        q = new MPMCLockFreeUnboundedQueue<>();
        final int PRODUCERS = 4;
        final int CONSUMERS = 4;
        final int OPS = 1000; // 每个生产者尝试放入的次数

        ExecutorService prodExec = Executors.newFixedThreadPool(PRODUCERS);
        ExecutorService consExec = Executors.newFixedThreadPool(CONSUMERS);

        ConcurrentLinkedQueue<Long> produced = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> consumed = new ConcurrentLinkedQueue<>();

        AtomicBoolean done = new AtomicBoolean(false);

        // 生产者任务
        for (int i = 0; i < PRODUCERS; i++) {
            final int id = i;
            prodExec.submit(() -> {
                for (int k = 0; k < OPS; k++) {
                    long val = System.nanoTime() ^ id;
                    q.offer(val);
                    produced.add(val);
                }
            });
        }

        // 消费者任务
        for (int i = 0; i < CONSUMERS; i++) {
            consExec.submit(() -> {
                // 简单的拉取循环，直到生产者完成大致数量的触发后退出
                int taken = 0;
                while (!done.get() || taken < OPS * PRODUCERS) {
                    Long v = q.poll();
                    if (v != null) {
                        consumed.add(v);
                        taken++;
                    } else {
                        // 轻微等待，避免空轮训占用 CPU
                        Thread.yield();
                    }
                }
            });
        }

        // 等待生产者完成
        prodExec.shutdown();
        prodExec.awaitTermination(20, TimeUnit.SECONDS);

        // 停止消费者
        done.set(true);
        consExec.shutdown();
        consExec.awaitTermination(20, TimeUnit.SECONDS);

        // 校验：消费的数量应等于生产的数量，且不重复（简单去重比较）
        assertEquals(produced.size(), consumed.size(), "生产与消费数量应相等");

        // 简单去重检测
        List<Long> prodList = new ArrayList<>(produced);
        for (Long v : consumed) {
            assertTrue(prodList.remove(v), "消费的值应来自生产者，且不多次消费");
        }
        System.out.println("epoch=" + q.PRODUCER_PRE_TOUCH_EPOCH.get());
        q = null;

    }

    // 测试轮询空队列的一致性
    @Test
    void testPollOnEmptyReturnsNull() {
        q=null;
        q = new MPMCLockFreeUnboundedQueue<>();
        assertNull(q.poll(), "空队列应返回 null");
    }

    // 测试重复 put / poll 的稳定性
    @Test
    void testManyPutPollCycles() {
        q=null;
        q = new MPMCLockFreeUnboundedQueue<>();
        int rounds = 1000;
        for (int i = 0; i < rounds; i++) {
            long v = i;
            q.offer(v);
            Long got = q.poll();
            assertNotNull(got);
            assertEquals(v, got.longValue());
        }
        // 再次确保空队列返回 null
        assertNull(q.poll());
    }
}
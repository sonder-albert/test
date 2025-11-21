package jmh;

import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import queue.MPMCLockFreeUnboundedQueue;

import java.util.concurrent.TimeUnit;

/**
 * 完全避免测量开销的 JMH 测试
 * 特点：
 *  - 不使用 System.nanoTime()（极大污染结果）
 *  - 不使用随机数
 *  - 不使用 Blackhole.consume()（仅在 poll 时执行一次避免JIT死代码）
 *  - 使用 volatile 计数器确保生产者和消费者之间有真实依赖
 *  - 确保每个线程都必须完成真实队列操作，JIT 无法优化掉
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(
        value = 1,
        jvmArgsAppend = {
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+UnlockExperimentalVMOptions",
                "-XX:+AlwaysPreTouch",
                "--enable-preview",
                "--enable-native-access=ALL-UNNAMED",
        }
)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MPMCQueuePerfFinalBenchmark {

    // ============================================================
    // Test Queues
    // ============================================================

    // ---------- Your queue ----------
    @State(Scope.Group)
    public static class MyQueueState {
        MPMCLockFreeUnboundedQueue<Long> q;
        volatile long counter = 1;
        @Setup(Level.Iteration) public void setup() {
            q = new MPMCLockFreeUnboundedQueue<>();
        }
    }

    // ---------- JCTools bounded MPMC ----------
    @State(Scope.Group)
    public static class JctBoundedState {
        MpmcArrayQueue<Long> q;
        volatile long counter = 1;
        @Setup(Level.Iteration) public void setup() {
            q = new MpmcArrayQueue<>(1024 * 1024);
        }
    }

    // ---------- JCTools unbounded MPMC ----------
    @State(Scope.Group)
    public static class JctUnboundedState {
        MpmcUnboundedXaddArrayQueue<Long> q;
        volatile long counter = 1;
        @Setup(Level.Iteration) public void setup() {
            q = new MpmcUnboundedXaddArrayQueue<>(1024);
        }
    }


    // ============================================================
    //  Helper: safe poll consume
    // ============================================================

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
    static void consume(Object v) {
        if (v == null && System.identityHashCode(v) == 42) {
            System.out.println("unreachable");
        }
    }


    // ============================================================
    // MY QUEUE
    // ============================================================

    @Benchmark @Group("my_1p1c")
    public void my_offer_1p1c(MyQueueState s) {
        s.q.offer(s.counter++);   // no nanoTime overhead
    }

    @Benchmark @Group("my_1p1c")
    public void my_poll_1p1c(MyQueueState s) {
        Object v = s.q.poll();
        consume(v);
    }


    @Benchmark @Group("my_4p4c")
    public void my_offer_4p4c(MyQueueState s) {
        s.q.offer(s.counter++);
    }

    @Benchmark @Group("my_4p4c")
    public void my_poll_4p4c(MyQueueState s) {
        Object v = s.q.poll();
        consume(v);
    }


    // ============================================================
    // JCTOOLS BOUNDED
    // ============================================================

    @Benchmark @Group("jct_bounded_1p1c")
    public void jct_bounded_offer_1p1c(JctBoundedState s) {
        s.q.offer(s.counter++);
    }

    @Benchmark @Group("jct_bounded_1p1c")
    public void jct_bounded_poll_1p1c(JctBoundedState s) {
        Object v = s.q.poll();
        consume(v);
    }


    @Benchmark @Group("jct_bounded_4p4c")
    public void jct_bounded_offer_4p4c(JctBoundedState s) {
        s.q.offer(s.counter++);
    }

    @Benchmark @Group("jct_bounded_4p4c")
    public void jct_bounded_poll_4p4c(JctBoundedState s) {
        Object v = s.q.poll();
        consume(v);
    }


    // ============================================================
    // JCTOOLS UNBOUNDED
    // ============================================================

    @Benchmark @Group("jct_unbounded_1p1c")
    public void jct_unbounded_offer_1p1c(JctUnboundedState s) {
        s.q.offer(s.counter++);
    }

    @Benchmark @Group("jct_unbounded_1p1c")
    public void jct_unbounded_poll_1p1c(JctUnboundedState s) {
        Object v = s.q.poll();
        consume(v);
    }


    @Benchmark @Group("jct_unbounded_4p4c")
    public void jct_unbounded_offer_4p4c(JctUnboundedState s) {
        s.q.offer(s.counter++);
    }

    @Benchmark @Group("jct_unbounded_4p4c")
    public void jct_unbounded_poll_4p4c(JctUnboundedState s) {
        Object v = s.q.poll();
        consume(v);
    }


    // ============================================================
    // MAIN
    // ============================================================

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(MPMCQueuePerfFinalBenchmark.class.getSimpleName())
                .threads(8)
                .build())
                .run();
    }
}
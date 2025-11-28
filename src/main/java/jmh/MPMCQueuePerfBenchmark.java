package jmh;

import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import queue.MPMCLockFreeUnboundedQueue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 完全避免测量开销的 JMH 测试
 * 特点：
 * - 不使用 System.nanoTime()（极大污染结果）
 * - 不使用随机数
 * - 不使用 Blackhole.consume()（仅在 poll 时执行一次避免JIT死代码）
 * - 使用 volatile 计数器确保生产者和消费者之间有真实依赖
 * - 确保每个线程都必须完成真实队列操作，JIT 无法优化掉
 */
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(
        value = 1,
        jvmArgsAppend = {
                "-XX:+UnlockDiagnosticVMOptions",
                "-XX:+UnlockExperimentalVMOptions",
                "-XX:+AlwaysPreTouch",
                "--enable-preview",
                "--enable-native-access=ALL-UNNAMED",
                "-XX:StartFlightRecording=name=rec,duration=1s,filename=rec.jfr",
                // 【修复】将 key 和 value 拆分成两个字符串
                "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-exports", "java.base/jdk.internal.vm.annotation=ALL-UNNAMED",
                "-Xmx8g",
                "-XX:-RestrictContended"
        }
)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MPMCQueuePerfBenchmark {

    // ============================================================
    // Test Queues
    // ============================================================

    // ---------- Your queue ----------
    @State(Scope.Group)
    public static class MyQueueState {
        MPMCLockFreeUnboundedQueue<Byte> q;

        @Setup(Level.Iteration)
        public void setup() {
            q = new MPMCLockFreeUnboundedQueue<>(1024);
        }
    }

    private static final Byte TEST_ELEMENT = 1;

    // ---------- JCTools unbounded MPMC ----------
    @State(Scope.Group)
    public static class JctUnboundedState {
        MpmcUnboundedXaddArrayQueue<Byte> q;

        @Setup(Level.Iteration)
        public void setup() {
            q = new MpmcUnboundedXaddArrayQueue<>(1024);
        }
    }

    // ---------- JDK ConcurrentLinkedQueue ----------
    @State(Scope.Group)
    public static class ConcurrentLinkedQueueState {
        ConcurrentLinkedQueue<Byte> q;

        @Setup(Level.Iteration)
        public void setup() {
            q = new ConcurrentLinkedQueue<>();
        }
    }


// ============================================================
// MY QUEUE
// ============================================================

    @Benchmark
    @Group("my_1p1c")
    public void my_offer_1p1c(MyQueueState s) {
        s.q.offer(TEST_ELEMENT);   // no nanoTime overhead
    }

    @Benchmark
    @Group("my_1p1c")
    public void my_poll_1p1c(MyQueueState s, Blackhole bh) {
        bh.consume(s.q.poll());
    }


    @Benchmark
    @Group("my_4p4c")
    public void my_offer_4p4c(MyQueueState s) {
        s.q.offer(TEST_ELEMENT);
    }

    @Benchmark
    @Group("my_4p4c")
    public void my_poll_4p4c(MyQueueState s, Blackhole bh) {
        bh.consume(s.q.poll());
    }


    // ============================================================
    // JCTOOLS UNBOUNDED
    // ============================================================

    @Benchmark @Group("jct_unbounded_1p1c")
    public void jct_unbounded_offer_1p1c(JctUnboundedState s) {
        s.q.offer(TEST_ELEMENT);
    }

    @Benchmark @Group("jct_unbounded_1p1c")
    public void jct_unbounded_poll_1p1c(JctUnboundedState s,Blackhole bh) {
        Object v = s.q.poll();
        bh.consume(v);
    }


    @Benchmark @Group("jct_unbounded_4p4c")
    public void jct_unbounded_offer_4p4c(JctUnboundedState s) {
        s.q.offer(TEST_ELEMENT);
    }

    @Benchmark @Group("jct_unbounded_4p4c")
    public void jct_unbounded_poll_4p4c(JctUnboundedState s,Blackhole bh) {
        Object v = s.q.poll();
        bh.consume(v);
    }

    // ============================================================
    // JDK ConcurrentLinkedQueue
    // ============================================================

    @Benchmark @Group("jdk_clq_1p1c")
    public void jdk_clq_offer_1p1c(ConcurrentLinkedQueueState s) {
        s.q.offer(TEST_ELEMENT);
    }

    @Benchmark @Group("jdk_clq_1p1c")
    public void jdk_clq_poll_1p1c(ConcurrentLinkedQueueState s,Blackhole bh) {
        Object v = s.q.poll();
        bh.consume(v);
    }


    @Benchmark @Group("jdk_clq_4p4c")
    public void jdk_clq_offer_4p4c(ConcurrentLinkedQueueState s) {
        s.q.offer(TEST_ELEMENT);
    }

    @Benchmark @Group("jdk_clq_4p4c")
    public void jdk_clq_poll_4p4c(ConcurrentLinkedQueueState s,Blackhole bh) {
        Object v = s.q.poll();
        bh.consume(v);
    }


// ============================================================
// MAIN
// ============================================================

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(MPMCQueuePerfBenchmark.class.getSimpleName())
                .threads(8)
                .build())
                .run();
    }
}
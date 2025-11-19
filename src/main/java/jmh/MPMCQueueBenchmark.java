package jmh;


import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import queue.MPMCLockFreeUnboundedQueue;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 1, jvmArgsAppend = {
        "-XX:+UnlockDiagnosticVMOptions",
        "-XX:+UnlockExperimentalVMOptions",
        "-Djdk.attach.allowAttachSelf=true",
        "-XX:+AlwaysPreTouch",
        "--enable-native-access=ALL-UNNAMED",
        "-Dmacosx.library.validation=disable",
})
@State(Scope.Group)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class MPMCQueueBenchmark {


    MPMCLockFreeUnboundedQueue<Long> q;

    @Setup(Level.Trial)
    public void setup() {
        q = new MPMCLockFreeUnboundedQueue<>();
    }

    // --------------- PRODUCER ----------------

    @Benchmark
    @Group("mpmc")
    public void offer(Blackhole bh) {
        q.offer(System.nanoTime());
    }

    // --------------- CONSUMER ----------------

    @Benchmark
    @Group("mpmc")
    public void poll(Blackhole bh) {
        Long v = q.poll();
        if (v != null) bh.consume(v);
    }

    public static void main(String[] args) throws RunnerException {
        Options build = new OptionsBuilder()
                .include(MPMCQueueBenchmark.class.getSimpleName() + ".*")
                .addProfiler("gc")
                //add vm option "-Djava.library.path=absolute/path/to/this/project/resource/libasyncProfiler.dylib"
                .addProfiler("async", "output=flamegraph.html")
//                .addProfiler("perfasm")
//                .addProfiler("perf")
                .detectJvmArgs()
                .threads(4)
                .build();
        new Runner(build).run();
    }
}

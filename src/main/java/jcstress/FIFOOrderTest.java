package jcstress;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.II_Result;
import queue.MPMCLockFreeUnboundedQueue;

@JCStressTest
@State
@Outcome(id = "1, 2", expect = Expect.ACCEPTABLE, desc = "Full: Ordered")
@Outcome(id = "-1, 1", expect = Expect.ACCEPTABLE,desc = "Consumer outran producer by 1 poll")
@Outcome(id = "1, -1", expect = Expect.ACCEPTABLE, desc = "Partial: Saw 1, 2 not ready")
@Outcome(id = "-1, -1", expect = Expect.ACCEPTABLE, desc = "Empty: Too fast")
// 致命错误场景
@Outcome(id = "2, 1", expect = Expect.FORBIDDEN, desc = "Violation: Reordered (Stack behavior or race)")
@Outcome(id = "2, -1", expect = Expect.FORBIDDEN, desc = "Violation: Saw 2 but missed 1 (Lost data or reordered)")
@Outcome(id = "-1, 2", expect = Expect.FORBIDDEN, desc = "Violation: Missed 1 but saw 2 (Broken FIFO)")
@Outcome(id = "0, 0", expect = Expect.FORBIDDEN, desc = "Violation: Data corruption")
public class FIFOOrderTest {

    final MPMCLockFreeUnboundedQueue<Integer> q = new MPMCLockFreeUnboundedQueue<>();

    @Actor
    public void producer() {
        // 同一个线程内，严格保证 1 先于 2
        q.offer(1);
        q.offer(2);
    }

    @Actor
    public void consumer(II_Result r) {
        Integer v1 = q.poll();
        Integer v2 = q.poll();

        r.r1 = (v1 == null ? -1 : v1);
        r.r2 = (v2 == null ? -1 : v2);
    }
}
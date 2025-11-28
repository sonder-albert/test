package jcstress;

import org.openjdk.jcstress.annotations.*;
import org.openjdk.jcstress.infra.results.IIII_Result;
import queue.MPMCLockFreeUnboundedQueue;

@JCStressTest
@State
// 状态码 1: 通过。表示读取的数据合法，且没有发生重复消费
@Outcome(id = "1, 0, 0, 0", expect = Expect.ACCEPTABLE, desc = "Success: Valid execution")

// 状态码 -1: 失败。表示同一个元素被多个消费者读到了 (CAS 失败保护无效)
@Outcome(id = "-1, 0, 0, 0", expect = Expect.FORBIDDEN, desc = "Error: Duplication detected")

// 状态码 -2: 失败。表示读到了意料之外的值 (内存可见性问题)
@Outcome(id = "-2, 0, 0, 0", expect = Expect.FORBIDDEN, desc = "Error: Data corruption detected")

// 兜底: 如果出现了其他奇怪的数字组合，也是禁止的
@Outcome(id = ".*", expect = Expect.FORBIDDEN, desc = "Error: Unexpected state")
public class MPMC4P4CTest {

    // 队列实例
    final MPMCLockFreeUnboundedQueue<Integer> q = new MPMCLockFreeUnboundedQueue<>(1024);

    // --- 4个生产者，分别放入不同的权重值 ---

    @Actor
    public void producer1() {
        q.offer(1);
    }

    @Actor
    public void producer2() {
        q.offer(10);
    }

    @Actor
    public void producer3() {
        q.offer(100);
    }

    @Actor
    public void producer4() {
        q.offer(1000);
    }

    // --- 4个消费者，将读到的值放入结果对象 ---

    @Actor
    public void consumer1(IIII_Result r) {
        Integer v = q.poll();
        r.r1 = (v == null) ? 0 : v;
    }

    @Actor
    public void consumer2(IIII_Result r) {
        Integer v = q.poll();
        r.r2 = (v == null) ? 0 : v;
    }

    @Actor
    public void consumer3(IIII_Result r) {
        Integer v = q.poll();
        r.r3 = (v == null) ? 0 : v;
    }

    @Actor
    public void consumer4(IIII_Result r) {
        Integer v = q.poll();
        r.r4 = (v == null) ? 0 : v;
    }

    // --- 仲裁者: 校验并生成状态码 ---
    // 这一步是在所有 Actor 执行完毕后，在主线程运行的
    @Arbiter
    public void arbiter(IIII_Result r) {
        // 1. 检查数据合法性 (Corruption Check)
        // 读到的值必须是 0(空), 1, 10, 100, 或 1000
        if (isInvalid(r.r1) || isInvalid(r.r2) || isInvalid(r.r3) || isInvalid(r.r4)) {
            r.r1 = -2; // 标记为数据损坏
            r.r2 = r.r3 = r.r4 = 0; // 清空其他以便匹配 Outcome
            return;
        }

        // 2. 计算总和
        int sum = r.r1 + r.r2 + r.r3 + r.r4;

        // 3. 检查重复消费 (Duplication Check)
        // 利用十进制特性：如果 P1 的数据 '1' 被两个消费者读到了，sum 的个位就会是 2
        int p1Count = sum % 10;
        int p2Count = (sum / 10) % 10;
        int p3Count = (sum / 100) % 10;
        int p4Count = (sum / 1000) % 10;

        if (p1Count > 1 || p2Count > 1 || p3Count > 1 || p4Count > 1) {
            r.r1 = -1; // 标记为数据重复
            r.r2 = r.r3 = r.r4 = 0;
            return;
        }

        // 4. 成功
        // 只要没有坏数据，也没有重复数据，即使 sum < 1111 (说明有数据还在队列里没读出来)，
        // 在并发测试中也视为 "ACCEPTABLE" (因为调度不可控，消费者可能在生产者之前就跑完了)
        r.r1 = 1;
        r.r2 = r.r3 = r.r4 = 0;
    }

    private boolean isInvalid(int v) {
        return v != 0 && v != 1 && v != 10 && v != 100 && v != 1000;
    }
}
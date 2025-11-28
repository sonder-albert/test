import queue.MPMCLockFreeUnboundedQueue;

import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentDebugSample {
    static AtomicLong counter = new AtomicLong(0);
    final static Byte TEST_ELEMENT = 0;

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        MPMCLockFreeUnboundedQueue<Object> queue = new MPMCLockFreeUnboundedQueue<>(1024);

        int PRODUCERS = 4;
        int CONSUMERS = 4;
        for (int p=0; p<PRODUCERS; p++) {
            new Thread(() -> {
                while (true) {
                    queue.offer(TEST_ELEMENT);
                }
            }).start();
        }
        for (int c=0; c<CONSUMERS; c++) {
            new Thread(() -> {
                while (true) {
                   queue.poll();
                    if (counter.getAndIncrement() % 10000 == 0) {
                        System.out.println("Consumed: " + counter.get());
                    }
                }
            }).start();
        }

        System.out.println("Test started...");
    }

}

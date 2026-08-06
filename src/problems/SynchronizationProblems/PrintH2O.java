package problems.SynchronizationProblems;

import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

public class PrintH2O {
    public static void main(String[] args) throws InterruptedException {
        ExecutorService executors = Executors.newFixedThreadPool(9);
        System.out.println("Print H2O with Barrier");
        printH2OWithBarrier(executors);
        executors.shutdown();
    }

    private static void printH2OWithBarrier(ExecutorService executors) {
        H2OWithBarrier obj = new H2OWithBarrier();
        executors.submit(() -> {
            obj.addHydrogen();;
        });
        executors.submit(() -> {
            obj.addOxygen();
        });
        executors.submit(() -> {
            obj.addOxygen();
        });
        executors.submit(() -> {
            obj.addHydrogen();
        });
        executors.submit(() -> {
            obj.addHydrogen();
        });
        executors.submit(() -> {
            obj.addHydrogen();
        });
        executors.submit(() -> {
            obj.addOxygen();
        });
        executors.submit(() -> {
            obj.addHydrogen();
        });
        executors.submit(() -> {
            obj.addHydrogen();
        });
    }

}
class H2OWithBarrier{
    private Semaphore hydrogenPermit = new Semaphore(2);
    private Semaphore oxygenPermit = new Semaphore(1);
    private CyclicBarrier barrier = new CyclicBarrier(3, () -> {
        System.out.println("Water molecule formed!");
        hydrogenPermit.release(2);
        oxygenPermit.release(1);
    });

    public void addHydrogen() {
        try {
            hydrogenPermit.acquire();
            barrier.await();
        } catch (BrokenBarrierException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void addOxygen() {
        try {
            oxygenPermit.acquire();
            barrier.await();
        } catch (BrokenBarrierException | InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

class H2OWithSemaphores{
    private final Semaphore hydrogenPermit = new Semaphore(2);
    private final Semaphore oxygenPermit = new Semaphore(1);
    private final ReentrantLock mutex = new ReentrantLock();
    private int hydrogen, oxygen;

    public void addHydrogen() {
        try {
            hydrogenPermit.acquire();
            mutex.lock();
            try {
                hydrogen++;
                if (hydrogen > 1 && oxygen > 0) {
                    hydrogen -= 2;
                    oxygen--;
                    System.out.println("water formed");
                    oxygenPermit.release(1);
                    hydrogenPermit.release(2);
                }
            } finally {
                mutex.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void addOxygen() {
        try {
            oxygenPermit.acquire();
            mutex.lock();
            try {
                oxygen++;
                if (hydrogen > 1 && oxygen > 0) {
                    hydrogen -= 2;
                    oxygen--;
                    System.out.println("water formed");
                    oxygenPermit.release(1);
                    hydrogenPermit.release(2);
                }
            } finally {
                mutex.unlock();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

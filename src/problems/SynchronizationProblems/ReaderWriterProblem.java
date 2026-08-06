package problems.SynchronizationProblems;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ReaderWriterProblem {

    // Read heavy application
    static class ReadHeavyApplication {
        private final Lock mutex = new ReentrantLock(true);
        private final Semaphore permit = new Semaphore(1, true);
        private final AtomicInteger readerCount = new AtomicInteger(0);

        private boolean readAcquire() {
            try {
                mutex.lock();
                if (readerCount.incrementAndGet() == 1) {
                    permit.acquire();
                }
            } catch (InterruptedException e) {
                readerCount.decrementAndGet();
                Thread.currentThread().interrupt();
                return false;
            } finally {
                mutex.unlock();
            }
            return true;
        }

        private void readRelease() {
            if (readerCount.decrementAndGet() == 0) {
                permit.release();
            }
        }

        public void read() {
            if (readAcquire()) {
                System.out.println("Read Resource!");
                readRelease();
            }
        }

        public void write() {
            mutex.lock();
            try {
                permit.acquire();
                System.out.println("Update Resource!");
                permit.release();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                mutex.unlock();
            }

        }
    }

    static class ReadHeavyApplication2 {
        private final Lock mutex = new ReentrantLock(true);
        private final Condition canRead = mutex.newCondition();
        private final Condition canWrite = mutex.newCondition();
        private int readerCnt;
        private boolean writing;

        public void read() {
            mutex.lock();
            try {
                while (writing) {
                    canRead.await();
                }
                readerCnt++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } finally {
                mutex.unlock();
            }
            try {
                Thread.sleep(50);
                System.out.println("Read completed!");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                readRelease();
            }
        }

        private void readRelease() {
            mutex.lock();
            if (--readerCnt == 0) {
                canWrite.signal();
            }
            mutex.unlock();
        }

        public void write() {
            mutex.lock();
            try {
                while (writing || readerCnt > 0) {
                    canWrite.await();
                }
                writing = true;
                Thread.sleep(50);
                System.out.println("Write completed!");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                writing = false;
                canRead.signalAll();
                canWrite.signalAll();
                mutex.unlock();
            }
        }
    }
}


class WriterHeavy {
    private int readerCnt = 0;
    private int writerCnt = 0;
    private Lock readerLock = new ReentrantLock();
    private Lock writerLock = new ReentrantLock();
    private Semaphore resource = new Semaphore(1);
    private Semaphore readTry = new Semaphore(1);

    private void readAcquire() throws InterruptedException {
        readTry.acquire();
        try {
            readerLock.lock();
            try {
                readerCnt++;
                if (readerCnt == 1) {
                    resource.acquire();
                }
            } finally {
                readerLock.unlock();
            }
        } finally {
            readTry.release();
        }
    }

    private void readRelease() {
        readerLock.lock();
        if (--readerCnt == 0) {
            resource.release();
        }
        readerLock.unlock();
    }

    public void readResource(Runnable task) {
        try {
            readAcquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        task.run();
        readRelease();
    }

    public void writeResource(Runnable task) {
        try {
            writeAcquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        task.run();
        writeRelease();
    }

    private void writeAcquire() throws InterruptedException {
        writerLock.lock();
        try {
            writerCnt++;
            if (writerCnt == 1) {
                readTry.acquire();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            writerLock.unlock();
        }
        resource.acquire();
    }

    private void writeRelease() {
        writerLock.lock();
        if (--writerCnt == 0) {
            readTry.release();
            resource.release();
        }
    }
}

class FairReadWrite {
    private int readCount = 0;
    private Lock lock = new ReentrantLock();
    private Semaphore resource = new Semaphore(1);
    private Semaphore serviceQueue = new Semaphore(1, true);

    private void readAcquire() {
        try {
            serviceQueue.acquire();
            lock.lock();
            try {
                if (++readCount == 1) {
                    resource.acquire();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
            serviceQueue.release();
        }
    }

    private void readRelease() {
        lock.lock();
        if (--readCount == 0) {
            resource.release();
        }
    }

    public void readResource(Runnable task) {
        readAcquire();
        task.run();
        readRelease();
    }

    public void writeResource(Runnable task) {
        try {
            serviceQueue.acquire();
            resource.acquire();
            task.run();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        resource.release();
        serviceQueue.release();
    }
}
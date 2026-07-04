package problems.SynchronizationProblems;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

public class LC1116 {
    private int n;
    private Lock lock;
    private boolean isZero;
    private int count;
    private Condition condition;

    public LC1116(int n){
        this.n = n;
        isZero = true;
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    // printNumber.accept(x) outputs "x", where x is an integer.
    public void zero(IntConsumer printNumber) throws InterruptedException {
        for (int i = 0; i < n; i++) {
            lock.lock();
            try {
                while (!isZero) condition.await();
                printNumber.accept(0);
                count++;
                isZero = false;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public void even(IntConsumer printNumber) throws InterruptedException {
        for (int i = 0; i < n/2; i++) {
            lock.lock();
            try {
                while (isZero || count % 2 != 0) condition.await();
                printNumber.accept(count);
                isZero = true;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public void odd(IntConsumer printNumber) throws InterruptedException {
        for (int i = 0; i < (n+1)/2; i++) {
            lock.lock();
            try {
                while (isZero || count % 2 == 0) condition.await();
                printNumber.accept(count);
                isZero = true;
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}

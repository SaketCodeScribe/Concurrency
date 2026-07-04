package problems.SynchronizationProblems;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PrintZeroEvenOdd {
    public static void main(String[] args) throws InterruptedException {
        ExecutorService executors = Executors.newFixedThreadPool(3);
        System.out.println("Print Zero Even Odd using condition variables");
        zeroEvenOddWithCV(executors);
        Thread.sleep(1000);
        System.out.println("\n\nPrint Zero Even Odd using semaphores");
        zeroEvenOddWithSemaphores(executors);
        executors.shutdown();
    }
    private static void zeroEvenOddWithSemaphores(ExecutorService executors) {
        ZeroEvenOddWithSemaphores obj = new ZeroEvenOddWithSemaphores();
        executors.submit(() -> {
            for(int i=1; i<=40; i++){
                obj.runZero();
            }
        });
        executors.submit(() -> {
            for(int i=1; i<=20; i++){
                obj.runOdd();
            }
        });
        executors.submit(() -> {
            for(int i=1; i<=20; i++){
                obj.runEven();
            }
        });
    }

    private static void zeroEvenOddWithCV(ExecutorService executors) {
        ZeroEvenOddWithConditionVariable obj = new ZeroEvenOddWithConditionVariable();
        executors.submit(() -> {
            for(int i=1; i<=10; i++){
                obj.runZero();
            }
        });
        executors.submit(() -> {
            for(int i=1; i<=5; i++){
                obj.runOdd();
            }
        });
        executors.submit(() -> {
            for(int i=1; i<=5; i++){
                obj.runEven();
            }
        });
    }
}
class ZeroEvenOddWithSemaphores{
    private int phase = 1;
    private Lock lock = new ReentrantLock();
    private Semaphore zeroPermit = new Semaphore(1);
    private Semaphore evenPermit = new Semaphore(0);
    private Semaphore oddPermit = new Semaphore(0);

    public void runZero(){
        try{
            zeroPermit.acquire();
            System.out.print(0);
            if (phase%2 != 0){
                oddPermit.release();
            }
            else{
                evenPermit.release();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void runEven(){
        try{
            evenPermit.acquire();
            System.out.print(phase);
            phase++;
            zeroPermit.release();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
    public void runOdd(){
        try{
            oddPermit.acquire();
            System.out.print(phase);
            phase++;
            zeroPermit.release();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
class ZeroEvenOddWithConditionVariable{
    private int phase = 1;
    private boolean zeroTurn = true;
    private Lock lock = new ReentrantLock();
    private Condition zero = lock.newCondition();
    private Condition odd = lock.newCondition();
    private Condition even = lock.newCondition();

    public void runZero(){
        lock.lock();
        try{
            while(!zeroTurn){
                zero.await();
            }
            System.out.print(0);
            zeroTurn = false;
            if (phase%2 != 0) {
                odd.signal();
            }
            else {
                even.signal();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            lock.unlock();
        }
    }
    public void runEven(){
        lock.lock();
        try{
            while(phase%2 != 0 || zeroTurn){
                even.await();
            }
            System.out.print(phase);
            phase++;
            zeroTurn = true;
            zero.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            lock.unlock();
        }
    }
    public void runOdd(){
        lock.lock();
        try{
            while(phase%2 == 0 || zeroTurn){
                odd.await();
            }
            System.out.print(phase);
            phase++;
            zeroTurn = true;
            zero.signal();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            lock.unlock();
        }
    }
}

class ZeroEvenOdd{
    private static Lock lock = new ReentrantLock();
    private static Condition condition = lock.newCondition();
    private static int count = 0;
    private static boolean isZero = true;
    private static final int n = 6;
    public static void main(String[] args) throws InterruptedException {
        Thread zero = new Thread(ZeroEvenOdd::printZero);
        Thread odd = new Thread(() -> {
            printNumber(true);
        });
        Thread even = new Thread(() -> {
            printNumber(false);
        });
        zero.start();
        odd.start();
        even.start();
        zero.join();
        odd.join();
        even.join();;
    }

    private static void printNumber(boolean odd) {
        int times = odd ? (n+1)/2 : n/2;
        for(int i=0; i<times; i++) {
            lock.lock();
            try {
                while (isZero || (count%2 == 0) == odd) {
                    condition.await();
                }
                System.out.print(count);
                isZero = true;
                condition.signalAll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
    }

    private static void printZero() {
        for(int i=0; i<n; i++) {
            lock.lock();
            try {
                while (!isZero) {
                    condition.await();
                }
                System.out.print(0);
                isZero = false;
                count++;
                condition.signalAll();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }
    }
}
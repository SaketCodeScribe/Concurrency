package problems.SynchronizationProblems;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;

public class LC1195 {
    class FizzBuzz {
        private int n;
        private Lock lock;
        private Condition condition;
        private int count;

        public FizzBuzz(int n) {
            this.n = n;
            count = 1;
            lock = new ReentrantLock();
            condition = lock.newCondition();
        }

        // printFizz.run() outputs "fizz".
        public void fizz(Runnable printFizz) throws InterruptedException {
            for(int i=0; i<n/3-n/15; i++){
                lock.lock();
                try{
                    while (count % 3 != 0 || count % 5 == 0){
                        condition.await();
                    }
                    count++;
                    printFizz.run();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }

        // printBuzz.run() outputs "buzz".
        public void buzz(Runnable printBuzz) throws InterruptedException {
            for(int i=0; i<n/5-n/15; i++){
                lock.lock();
                try{
                    while (count % 5 != 0 || count % 3 == 0){
                        condition.await();
                    }
                    count++;
                    printBuzz.run();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }

        // printFizzBuzz.run() outputs "fizzbuzz".
        public void fizzbuzz(Runnable printFizzBuzz) throws InterruptedException {
            for(int i=0; i<n/15; i++){
                lock.lock();
                try{
                    while (!(count % 3 == 0 && count % 5 == 0)){
                        condition.await();
                    }
                    count++;
                    printFizzBuzz.run();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }

        // printNumber.accept(x) outputs "x", where x is an integer.
        public void number(IntConsumer printNumber) throws InterruptedException {
            int multipleFifteen = n/15;
            int multipleFive = n/5;
            int multipleThree = n/3;
            int times =  n - multipleFive - multipleThree + multipleFifteen;
            for(int i=0; i<times; i++){
                lock.lock();
                try{
                    while (count % 3 == 0 || count % 5 == 0){
                        condition.await();
                    }
                    printNumber.accept(count++);
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
    }
}

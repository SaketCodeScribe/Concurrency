package problems.SynchronizationProblems;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProblemUnisexBathroom {
    private final Lock lock = new ReentrantLock(true);
    private final Condition canMaleEnter = lock.newCondition();
    private final Condition canFemaleEnter = lock.newCondition();
    private char switchGender = 0;
    private final int capacity;
    private int maleCnt, femaleCnt, maleWaiting, femaleWaiting;

    public ProblemUnisexBathroom(int capacity) {
        this.capacity = capacity;
    }

    public void useWashRoom(char gender) throws InterruptedException {
        enter(gender);
        try {
            System.out.println(gender + " using bathroom");
            Thread.sleep(1000);
        } finally {
            leave(gender);
        }
    }

    private void enter(char gender) throws InterruptedException {
        lock.lock();
        try {
            if (gender == 'M') {
                maleWaiting++;
                while (femaleCnt > 0 || maleCnt == this.capacity || switchGender == 'F') {
                    canMaleEnter.await();
                }
                maleWaiting--;
                maleCnt++;
            } else {
                femaleWaiting++;
                while (maleCnt > 0 || femaleCnt == this.capacity || switchGender == 'M') {
                    canFemaleEnter.await();
                }
                femaleWaiting--;
                femaleCnt++;
            }
        } catch (InterruptedException e) {
            if (gender == 'M') maleWaiting--;
            if (gender == 'F') femaleWaiting--;
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private void leave(char gender) {
        lock.lock();
        try {
            if (gender == 'M') maleCnt--;
            else femaleCnt--;
            if (maleCnt == 0 && femaleCnt == 0) {
                if (gender == 'F' && maleWaiting > 0) {
                    canMaleEnter.signalAll();
                    switchGender = 'M';
                } else if (gender == 'M' && femaleWaiting > 0) {
                    canFemaleEnter.signalAll();
                    switchGender = 'F';
                } else if (gender == 'M' && maleWaiting > 0) canMaleEnter.signalAll();
                else if (gender == 'F' && femaleWaiting > 0) canFemaleEnter.signalAll();
                else switchGender = 0;
            } else if (gender == 'M' && femaleWaiting == 0 && maleWaiting > 0 && maleCnt < this.capacity) canMaleEnter.signalAll();
            else if (gender == 'F' && maleWaiting == 0 && femaleWaiting > 0 && femaleCnt < this.capacity) canFemaleEnter.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

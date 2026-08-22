package problems.SynchronizationProblems;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ProblemUnisexBathroom {
    enum Gender {
        Male,
        Female;
    }
    private final Lock lock = new ReentrantLock(true);
    private final Condition canMaleEnter = lock.newCondition();
    private final Condition canFemaleEnter = lock.newCondition();
    private Gender switchGender = null;
    private final int capacity;
    private int maleCnt, femaleCnt, maleWaiting, femaleWaiting;

    public ProblemUnisexBathroom(int capacity) {
        this.capacity = capacity;
    }

    public void useWashRoom(Gender gender) throws InterruptedException {
        enter(gender);
        try {
            System.out.println(gender.toString() + " using bathroom");
            Thread.sleep(1000);
        } finally {
            leave(gender);
        }
    }

    private void enter(Gender gender) throws InterruptedException {
        lock.lock();
        try {
            if (gender == Gender.Male) {
                maleWaiting++;
                while (femaleCnt > 0 || maleCnt == this.capacity || switchGender == Gender.Female) {
                    canMaleEnter.await();
                }
                maleWaiting--;
                maleCnt++;
            } else {
                femaleWaiting++;
                while (maleCnt > 0 || femaleCnt == this.capacity || switchGender == Gender.Male) {
                    canFemaleEnter.await();
                }
                femaleWaiting--;
                femaleCnt++;
            }
        } catch (InterruptedException e) {
            if (gender == Gender.Male) maleWaiting--;
            if (gender == Gender.Female) femaleWaiting--;
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    private void leave(Gender gender) {
        lock.lock();
        try {
            if (gender == Gender.Male) maleCnt--;
            else femaleCnt--;
            if (maleCnt == 0 && femaleCnt == 0) {
                if (gender == Gender.Female && maleWaiting > 0) {
                    canMaleEnter.signalAll();
                    switchGender = Gender.Male;
                } else if (gender == Gender.Male && femaleWaiting > 0) {
                    canFemaleEnter.signalAll();
                    switchGender = Gender.Female;
                } else if (gender == Gender.Male && maleWaiting > 0) canMaleEnter.signalAll();
                else if (femaleWaiting > 0) canFemaleEnter.signalAll();
                else switchGender = null;
            } else if (gender == Gender.Male && femaleWaiting == 0 && maleWaiting > 0 && maleCnt < this.capacity) canMaleEnter.signalAll();
            else if (gender == Gender.Female && maleWaiting == 0 && femaleWaiting > 0 && femaleCnt < this.capacity) canFemaleEnter.signalAll();
        } finally {
            lock.unlock();
        }
    }
}

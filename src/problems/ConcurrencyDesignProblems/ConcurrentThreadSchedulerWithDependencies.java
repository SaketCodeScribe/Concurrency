package problems.ConcurrencyDesignProblems;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentThreadSchedulerWithDependencies {
    static class Task{
        String taskId;
        Runnable task;
        List<Task> dependents;
        AtomicInteger dependencyCount;

        public Task(String taskId, Runnable task) {
            this.taskId = taskId;
            this.task = task;
            dependents = new ArrayList<>();
            dependencyCount = new AtomicInteger(0);
        }
        public int decrementDependenciesCount(){
            return dependencyCount.decrementAndGet();
        }
        public void decrementDependents(){
            for(Task t: dependents){
                t.decrementDependenciesCount();
            }
        }
        public void run(){
            task.run();
        }
        public void add(Task t){
            dependents.add(t);
        }
        public int getDependencyCount(){
            return dependencyCount.get();
        }

        public void incrementDependenciesCount() {
            dependencyCount.incrementAndGet();
        }
    }
    private final Set<Task> tasks;
    private final Lock lock;
    private final Condition condition;
    DependencyScheduler scheduler;
    public ConcurrentThreadSchedulerWithDependencies(List<List<Task>> items) {
        lock = new ReentrantLock();
        condition = lock.newCondition();
        tasks = ConcurrentHashMap.newKeySet();

        for(List<Task> item:items){
            Task a = item.get(0);
            Task b = item.get(1);
            b.add(a);
            a.incrementDependenciesCount();
            tasks.add(a);
            tasks.add(b);
        }
        scheduler = new DependencyScheduler(10);
    }

    public void start(){
        scheduler.submit(tasks);
    }
    public void close(){
        scheduler.cancel();
    }
    static class DependencyScheduler{
        ExecutorService workerPool;
        BlockingQueue<Task> readyQueue;
        Set<Task> tasks;
        private final Lock lock;
        private final Condition waitCondition;
        AtomicBoolean running;
        int poolSize;
        public DependencyScheduler(int poolSize) {
            this.poolSize = poolSize;
            lock = new ReentrantLock();
            waitCondition = lock.newCondition();
            this.workerPool = Executors.newFixedThreadPool(poolSize, (r) -> {
                Thread th = new Thread(r, "Worker Thread");
                th.setDaemon(true);
                return th;
            });
            readyQueue = new LinkedBlockingQueue<>();
            running = new AtomicBoolean(false);
            tasks = ConcurrentHashMap.newKeySet();
        }

        public void submit(Set<Task> tasks){
            if (running.compareAndSet(false, true)) {
                tasks.forEach(task -> {
                    if (task.getDependencyCount() == 0)
                        readyQueue.offer(task);
                    this.tasks.add(task);
                });
                for(int i = 0; i<poolSize; i++){
                    workerPool.submit(this::run);
                }
            }
        }
        private void run() {
            try {
                while (running.get() && !tasks.isEmpty()) {
                    Task task = (Task) readyQueue.take();
                    task.run();
                    onComplete(task);
                }
            } catch (InterruptedException e) {
                cancel();
                throw new RuntimeException(e);
            }
        }

        private void onComplete(Task task) {
            for(Task dependent:task.dependents){
                if (dependent.decrementDependenciesCount() == 0){
                    readyQueue.offer(dependent);
                }
            }
            this.tasks.remove(task);
            if (tasks.isEmpty()) cancel();
        }
        public void cancel(){
            shutdown();
        }
        private void shutdown(){
            running.set(false);
            workerPool.shutdown();
            try {
                workerPool.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.*;

/**
 * Высокопроизводительный пул потоков с настраиваемым управлением очередями
 * Улучшенная версия с исправлениями архитектурных проблем
 */
public class CustomThreadPool implements CustomExecutor {
    // private static final Logger logger = Logger.getLogger(CustomThreadPool.class.getName());
    
    // Основные параметры пула
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    
    // Очереди и потоки
    private final List<BlockingQueue<Runnable>> taskQueues;
    private final Set<Worker> workers;
    private final CustomThreadFactory threadFactory;
    private final RejectedExecutionHandler rejectionHandler;
    
    // Состояние пула
    private volatile boolean shutdown = false;
    private volatile boolean terminated = false;
    private final AtomicInteger activeThreadCount = new AtomicInteger(0);
    private final AtomicInteger totalThreadCount = new AtomicInteger(0);
    private final AtomicLong taskCounter = new AtomicLong(0);
    private final AtomicInteger queueSelector = new AtomicInteger(0);
    
    // Синхронизация
    private final ReentrantLock mainLock = new ReentrantLock();
    private final Object terminationLock = new Object();
    
    /**
     * Конструктор пула потоков
     */
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, 
                           TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        validateParameters(corePoolSize, maxPoolSize, keepAliveTime, queueSize, minSpareThreads);
        
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = Math.max(1, queueSize / corePoolSize); // Исправлено: защита от 0
        this.minSpareThreads = minSpareThreads;
        
        this.taskQueues = new ArrayList<>(corePoolSize);
        this.workers = Collections.synchronizedSet(new HashSet<>(maxPoolSize));
        this.threadFactory = new CustomThreadFactory();
        this.rejectionHandler = new CustomRejectedExecutionHandler();
        
        // Инициализация очередей
        for (int i = 0; i < corePoolSize; i++) {
            taskQueues.add(new LinkedBlockingQueue<>(this.queueSize));
        }
        
        // Создание базовых потоков
        createCoreWorkers();
        
        // logger.info(String.format("[Pool] CustomThreadPool initialized with core=%d, max=%d, keepAlive=%d %s, queuePerCore=%d",
        //         corePoolSize, maxPoolSize, keepAliveTime, timeUnit, this.queueSize));
    }
    
    private void validateParameters(int corePoolSize, int maxPoolSize, long keepAliveTime, 
                                  int queueSize, int minSpareThreads) {
        if (corePoolSize < 0 || maxPoolSize <= 0 || maxPoolSize < corePoolSize)
            throw new IllegalArgumentException("Invalid pool sizes: core=" + corePoolSize + ", max=" + maxPoolSize);
        if (keepAliveTime < 0)
            throw new IllegalArgumentException("Keep alive time cannot be negative");
        if (queueSize <= 0)
            throw new IllegalArgumentException("Queue size must be positive");
        if (minSpareThreads < 0 || minSpareThreads > maxPoolSize)
            throw new IllegalArgumentException("Invalid minSpareThreads value: " + minSpareThreads);
    }
    
    private void createCoreWorkers() {
        mainLock.lock();
        try {
            for (int i = 0; i < corePoolSize && !shutdown; i++) {
                Worker worker = new Worker(taskQueues.get(i), i, true);
                workers.add(worker);
                worker.thread.start();
                totalThreadCount.incrementAndGet();
                // logger.info("[Pool] Created core worker for queue #" + i);
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public void execute(Runnable command) {
        if (command == null) throw new NullPointerException("Command cannot be null");
        
        if (shutdown) {
            rejectionHandler.rejectedExecution(command, null);
            return;
        }
        
        long taskId = taskCounter.incrementAndGet();
        TaskWrapper task = new TaskWrapper(command, taskId);
        
        // ИСПРАВЛЕНИЕ: Делаем только ОДНУ попытку размещения с fallback стратегиями
        if (tryPlaceTask(task)) {
            checkAndCreateSpareThreads();
            return;
        }
        
        // Если не удалось разместить - создаем дополнительный поток и пытаемся ЕЩЕ РАЗ
        if (createExtraWorkerIfNeeded() && tryPlaceTask(task)) {
            return;
        }
        
        // Отклоняем задачу
        rejectionHandler.rejectedExecution(command, null);
    }

    // НОВЫЙ МЕТОД: Объединяет Least Loaded + Round Robin в одной попытке
    private boolean tryPlaceTask(TaskWrapper task) {
        // Стратегия 1: Least Loaded (ваш оригинальный алгоритм)
        int bestQueueIndex = findBestQueue();
        if (taskQueues.get(bestQueueIndex).offer(task)) {
            return true;
        }
        
        // Стратегия 2: Round Robin fallback - пробуем все остальные очереди
        for (int attempt = 0; attempt < taskQueues.size() - 1; attempt++) {
            int queueIndex = (queueSelector.getAndIncrement() % taskQueues.size());
            if (queueIndex != bestQueueIndex && taskQueues.get(queueIndex).offer(task)) {
                return true;
            }
        }
        
        return false; // Все очереди заполнены
    }
    
    /** 
     * Находит наименее загруженную очередь (Least Loaded алгоритм)
     */
    private int findBestQueue() {
        int bestQueue = 0;
        int minSize = Integer.MAX_VALUE;
        
        // Делаем снимок размеров всех очередей
        for (int i = 0; i < taskQueues.size(); i++) {
            int size = taskQueues.get(i).size();
            if (size < minSize) {
                minSize = size;
                bestQueue = i;
                if (size == 0) {
                    break; // Нашли пустую - лучше не будет
                }
            }
        }
        return bestQueue;
    }
    
    private void checkAndCreateSpareThreads() {
        int currentActive = activeThreadCount.get();
        int currentTotal = totalThreadCount.get();
        int availableThreads = currentTotal - currentActive;
        
        if (availableThreads < minSpareThreads && currentTotal < maxPoolSize) {
            createExtraWorkerIfNeeded();
        }
    }
    
    private boolean createExtraWorkerIfNeeded() {
        mainLock.lock();
        try {
            if (totalThreadCount.get() >= maxPoolSize || shutdown) {
                return false;
            }
            
            // Создаем дополнительный поток для наименее загруженной очереди
            int bestQueueIndex = findBestQueue();
            Worker worker = new Worker(taskQueues.get(bestQueueIndex), bestQueueIndex, false);
            workers.add(worker);
            worker.thread.start();
            totalThreadCount.incrementAndGet();
            
            // logger.info("[Pool] Created extra worker for queue #" + bestQueueIndex + 
            //            " (total threads: " + totalThreadCount.get() + ")");
            return true;
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        if (callable == null) throw new NullPointerException("Callable cannot be null");
        
        FutureTask<T> task = new FutureTask<>(callable);
        execute(task);
        return task;
    }
    
    public Future<?> submit(Runnable task) {
        if (task == null) throw new NullPointerException("Runnable cannot be null");
        
        FutureTask<Void> futureTask = new FutureTask<>(task, null);
        execute(futureTask);
        return futureTask;
    }
    
    @Override
    public void shutdown() {
        mainLock.lock();
        try {
            if (shutdown) return;
            
            shutdown = true;
            // logger.info("[Pool] Shutdown initiated - stopping acceptance of new tasks");
            
            // Прерываем все рабочие потоки для graceful shutdown
            synchronized (workers) {
                for (Worker worker : workers) {
                    worker.signalShutdown();
                }
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public void shutdownNow() {
        List<Runnable> remainingTasks = new ArrayList<>();
        mainLock.lock();
        try {
            if (!shutdown) {
                shutdown = true;
                // logger.info("[Pool] ShutdownNow initiated - forcing immediate termination");
                
                // Прерываем все рабочие потоки
                synchronized (workers) {
                    for (Worker worker : workers) {
                        worker.interrupt();
                    }
                }
                
                // Очищаем все очереди и возвращаем невыполненные задачи
                for (BlockingQueue<Runnable> queue : taskQueues) {
                    queue.drainTo(remainingTasks);
                }
                
                // logger.info("[Pool] ShutdownNow completed, " + remainingTasks.size() + " tasks cancelled");
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    public boolean isShutdown() {
        return shutdown;
    }
    
    public boolean isTerminated() {
        return terminated;
    }
    
    public int getActiveCount() {
        return activeThreadCount.get();
    }
    
    public int getPoolSize() {
        return totalThreadCount.get();
    }
    
    public long getTaskCount() {
        return taskCounter.get();
    }
    
    public int getQueueSize() {
        return taskQueues.stream().mapToInt(BlockingQueue::size).sum();
    }
    
    /**
     * Wrapper для задач с идентификатором и метриками
     */
    private static class TaskWrapper implements Runnable {
        private final Runnable task;
        private final long taskId;
        private final long submitTime;
        
        TaskWrapper(Runnable task, long taskId) {
            this.task = task;
            this.taskId = taskId;
            this.submitTime = System.currentTimeMillis();
        }
        
        @Override
        public void run() {
            long waitTime = System.currentTimeMillis() - submitTime;
            if (waitTime > 100) { // Логируем только если задача ждала более 100мс
                // logger.info("[Task] Task-" + taskId + " waited " + waitTime + "ms in queue");
            }
            task.run();
        }
        
        @Override
        public String toString() {
            return "Task-" + taskId;
        }
    }
    
    /**
     * Улучшенная фабрика потоков с детальным логированием
     */
    private class CustomThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix = "MyPool-worker-";
        
        @Override
        public Thread newThread(Runnable r) {
            String threadName = namePrefix + threadNumber.getAndIncrement();
            Thread thread = new Thread(r, threadName);
            thread.setDaemon(false);
            thread.setPriority(Thread.NORM_PRIORITY);
            
            // Добавляем UncaughtExceptionHandler для лучшего логирования ошибок
            thread.setUncaughtExceptionHandler((t, e) -> {
                // logger.log(Level.SEVERE, "[ThreadFactory] Uncaught exception in thread " + t.getName(), e);
            });
            
            // logger.info("[ThreadFactory] Creating new thread: " + threadName);
            return thread;
        }
    }
    
    /**
     * Обработчик отклоненных задач с подробным анализом причин
     */
    private class CustomRejectedExecutionHandler implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            String reason = shutdown ? "pool is shutting down" : "pool is overloaded";
            String details = String.format("active=%d, total=%d, queue=%d", 
                    getActiveCount(), getPoolSize(), getQueueSize());
            
            // logger.warning("[Rejected] Task rejected (" + reason + "): " + r + " [" + details + "]");
            
            // Выбрана стратегия AbortPolicy - быстрый отказ с исключением
            // Альтернативы:
            // - CallerRunsPolicy: выполнение в текущем потоке (может замедлить отправителя)
            // - DiscardPolicy: тихое отбрасывание (потеря задач)
            // - DiscardOldestPolicy: удаление старых задач (может нарушить порядок)
            throw new RejectedExecutionException("Task " + r.toString() + " rejected: " + reason);
        }
    }
    
    /**
     * Улучшенный рабочий поток с более надежной обработкой жизненного цикла
     */
    private class Worker implements Runnable {
        final Thread thread;
        private final BlockingQueue<Runnable> workQueue;
        private final int queueId;
        private final boolean isCoreWorker;
        private volatile boolean running = true;
        private volatile boolean shutdownSignaled = false;
        
        Worker(BlockingQueue<Runnable> queue, int queueId, boolean isCoreWorker) {
            this.workQueue = queue;
            this.queueId = queueId;
            this.isCoreWorker = isCoreWorker;
            this.thread = threadFactory.newThread(this);
        }
        
        @Override
        public void run() {
            try {
                while (running && !Thread.currentThread().isInterrupted()) {
                    Runnable task = getTask();
                    if (task != null) {
                        runTask(task);
                    } else {
                        // getTask() вернул null - анализируем причину
                        if (shouldTerminate()) {
                            break;
                        }
                        // Для core потоков продолжаем работу даже после timeout
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // logger.info("[Worker] " + Thread.currentThread().getName() + " interrupted");
            } finally {
                workerDone();
            }
        }
        
    private Runnable getTask() throws InterruptedException {
        boolean shutdownSnapshot = shutdown;  // Снимок состояния
        
        if (isCoreWorker) {
            if (shutdownSnapshot) {
                return workQueue.poll(); // Non-blocking при shutdown
            }
            return workQueue.poll(5, TimeUnit.SECONDS);
        } else {
            // Extra поток
            Runnable task = workQueue.poll(keepAliveTime, timeUnit);
            if (task == null && !shutdownSnapshot) {
                // Завершаемся только если не было shutdown во время ожидания
                return null;
            }
            return task;
        }
    }
        
        private void runTask(Runnable task) {
            activeThreadCount.incrementAndGet();
            long startTime = System.currentTimeMillis();
            
            try {
                // logger.info("[Worker] " + Thread.currentThread().getName() + " executes " + task);
                task.run();
                
                long executionTime = System.currentTimeMillis() - startTime;
                if (executionTime > 1000) { // Логируем медленные задачи
                    // logger.info("[Worker] Task " + task + " took " + executionTime + "ms");
                }
            } catch (Exception e) {
                // logger.log(Level.SEVERE, "[Worker] Task execution failed: " + task, e);
            } finally {
                activeThreadCount.decrementAndGet();
            }
        }
        
        private boolean shouldTerminate() {
            boolean shutdownSnapshot = shutdownSignaled;
            boolean queueEmptySnapshot = workQueue.isEmpty();
            
            if (isCoreWorker) {
                return shutdownSnapshot && queueEmptySnapshot;
            } else {
                return shutdownSnapshot && queueEmptySnapshot;
            }
        }
        
        private void workerDone() {
            mainLock.lock();
            try {
                workers.remove(this);
                int remaining = totalThreadCount.decrementAndGet();
                
                String workerType = isCoreWorker ? "core" : "extra";
                // logger.info("[Worker] " + Thread.currentThread().getName() + 
                //            " (" + workerType + ") terminated. Remaining threads: " + remaining);
                
                // Проверяем полное завершение пула
                if (remaining == 0 && shutdown) {
                    synchronized (terminationLock) {
                        terminated = true;
                        terminationLock.notifyAll();
                        // logger.info("[Pool] All workers terminated - pool fully shutdown");
                    }
                }
            } finally {
                mainLock.unlock();
            }
        }
        
        void signalShutdown() {
            shutdownSignaled = true;
            // Мягкое завершение - не прерываем поток принудительно
            // Поток завершится после обработки текущей задачи
        }
        
        void interrupt() {
            running = false;
            shutdownSignaled = true;
            thread.interrupt();
        }
    }
    
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        
        synchronized (terminationLock) {
            while (!terminated) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    // logger.warning("[Pool] Termination timeout exceeded. Active threads: " + getPoolSize());
                    return false;
                }
                
                long waitMillis = remaining / 1_000_000;
                int waitNanos = (int)(remaining % 1_000_000);
                terminationLock.wait(waitMillis, waitNanos);
            }
            return true;
        }
    }
    
    @Override
    public String toString() {
        return String.format("CustomThreadPool[Running=%s, Pool Size=%d/%d, Active=%d, Queued=%d, Completed=%d]",
                !shutdown, getPoolSize(), maxPoolSize, getActiveCount(), 
                getQueueSize(), getTaskCount());
    }
}

/**
 * Интерфейс кастомного исполнителя
 */
interface CustomExecutor extends Executor {
    void execute(Runnable command);
    <T> Future<T> submit(Callable<T> callable);
    void shutdown();
    void shutdownNow();
}
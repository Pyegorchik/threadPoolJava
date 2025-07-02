import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.SimpleFormatter;
import java.util.*;

/**
 * Демонстрационная программа для тестирования CustomThreadPool
 * Показывает различные сценарии использования и анализ производительности
 */
public class ThreadPoolDemo {
    private static final Logger logger = Logger.getLogger(ThreadPoolDemo.class.getName());
    
    static {
        setupLogging();
    }
    
    private static void setupLogging() {
        Logger rootLogger = Logger.getLogger("");
        rootLogger.setLevel(Level.INFO);
        rootLogger.setUseParentHandlers(false);
        
        // Удаляем существующие handlers
        for (java.util.logging.Handler handler : rootLogger.getHandlers()) {
            rootLogger.removeHandler(handler);
        }
        
        // Настраиваем консольный вывод
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        handler.setLevel(Level.INFO);
        rootLogger.addHandler(handler);
    }
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("=== ДЕМОНСТРАЦИЯ CustomThreadPool ===\n");
        
        // Тест 1: Базовая функциональность
        System.out.println("1. ТЕСТ БАЗОВОЙ ФУНКЦИОНАЛЬНОСТИ");
        testBasicFunctionality();
        
        // Тест 2: Обработка перегрузки
        System.out.println("\n2. ТЕСТ ОБРАБОТКИ ПЕРЕГРУЗКИ");
        testOverloadHandling();
        
        // Тест 3: Graceful shutdown
        System.out.println("\n3. ТЕСТ GRACEFUL SHUTDOWN");
        testGracefulShutdown();
        
        // Тест 4: Сравнение производительности
        System.out.println("\n4. СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ");
        performanceComparison();
        
        // Тест 5: Анализ параметров
        System.out.println("\n5. АНАЛИЗ ВЛИЯНИЯ ПАРАМЕТРОВ");
        parameterAnalysis();
        
        System.out.println("\n=== ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА ===");
    }
    
    /**
     * Тест базовой функциональности пула
     */
    private static void testBasicFunctionality() throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize
            4,  // maxPoolSize  
            5,  // keepAliveTime
            TimeUnit.SECONDS,
            10, // queueSize (общий размер, разделится между ядрами)
            1   // minSpareThreads
        );
        
        System.out.println("Создан пул: " + pool);
        
        // Отправляем задачи разной продолжительности
        for (int i = 1; i <= 6; i++) {
            final int taskId = i;
            pool.execute(new TestTask("Task-" + taskId, taskId * 500));
        }
        
        // Даем времени на выполнение
        Thread.sleep(3000);
        System.out.println("Статус после 3 сек: " + pool);
        
        // Тест Future
        Future<String> future = pool.submit(() -> {
            Thread.sleep(1000);
            return "Future result from " + Thread.currentThread().getName();
        });
        
        try {
            String result = future.get(2, TimeUnit.SECONDS);
            System.out.println("Future результат: " + result);
        } catch (Exception e) {
            System.out.println("Future ошибка: " + e.getMessage());
        }
        
        pool.shutdown();
        boolean terminated = pool.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("Пул завершен корректно: " + terminated);
        System.out.println("Финальная статистика: " + pool);
    }
    
    /**
     * Тест обработки перегрузки пула
     */
    private static void testOverloadHandling() throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(
            1,  // corePoolSize - специально мало
            2,  // maxPoolSize
            2,  // keepAliveTime
            TimeUnit.SECONDS,
            4,  // queueSize - маленькая очередь
            0   // minSpareThreads
        );
        
        System.out.println("Создан ограниченный пул: " + pool);
        
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        
        // Отправляем много задач для создания перегрузки
        for (int i = 1; i <= 15; i++) {
            try {
                final int taskId = i;
                pool.execute(() -> {
                    try {
                        System.out.println("Выполняется задача " + taskId + " в " + Thread.currentThread().getName());
                        Thread.sleep(1000);
                        successCount.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                System.out.println("Задача " + i + " принята. Статус: " + pool);
            } catch (RejectedExecutionException e) {
                rejectedCount.incrementAndGet();
                System.out.println("Задача " + i + " ОТКЛОНЕНА: " + e.getMessage());
            }
            
            Thread.sleep(100); // Небольшая пауза между отправками
        }
        
        Thread.sleep(8000); // Ждем завершения выполнения
        
        System.out.println("Результаты перегрузки:");
        System.out.println("- Успешно выполнено: " + successCount.get());
        System.out.println("- Отклонено: " + rejectedCount.get());
        System.out.println("- Финальный статус: " + pool);
        
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
    }
    
    /**
     * Тест корректного завершения пула
     */
    private static void testGracefulShutdown() throws InterruptedException {
        CustomThreadPool pool = new CustomThreadPool(2, 3, 3, TimeUnit.SECONDS, 6, 1);
        
        // Запускаем долгие задачи
        for (int i = 1; i <= 4; i++) {
            final int taskId = i;
            pool.execute(new TestTask("LongTask-" + taskId, 2000));
        }
        
        Thread.sleep(500); // Даем задачам запуститься
        System.out.println("Запущено 4 долгих задачи. Статус: " + pool);
        
        // Инициируем shutdown
        System.out.println("Инициируем graceful shutdown...");
        pool.shutdown();
        
        // Пытаемся добавить новую задачу после shutdown
        try {
            pool.execute(() -> System.out.println("Эта задача не должна выполниться"));
        } catch (RejectedExecutionException e) {
            System.out.println("Новая задача корректно отклонена: " + e.getMessage());
        }
        
        // Ждем завершения
        boolean terminated = pool.awaitTermination(8, TimeUnit.SECONDS);
        System.out.println("Graceful shutdown завершен: " + terminated);
        System.out.println("Все задачи выполнены, пул завершен корректно");
    }
    
    /**
     * Сравнение производительности с стандартным ThreadPoolExecutor
     */
    private static void performanceComparison() throws InterruptedException {
        final int TASK_COUNT = 1000;
        final int TASK_DURATION = 10; // мс
        
        System.out.println("Сравнение производительности на " + TASK_COUNT + " задачах:");
        
        // Тест CustomThreadPool
        long customTime = testPoolPerformance("CustomThreadPool", () -> {
            CustomThreadPool pool = new CustomThreadPool(4, 8, 5, TimeUnit.SECONDS, 50, 2);
            return new CustomExecutorWrapper(pool);
        }, TASK_COUNT, TASK_DURATION);
        
        // Тест стандартного ThreadPoolExecutor
        long standardTime = testPoolPerformance("ThreadPoolExecutor", () -> {
            ThreadPoolExecutor pool = new ThreadPoolExecutor(
                4, 8, 5, TimeUnit.SECONDS, 
                new LinkedBlockingQueue<>(50),
                new ThreadPoolExecutor.AbortPolicy()
            );
            return new StandardExecutorWrapper(pool);
        }, TASK_COUNT, TASK_DURATION);
        
        // Тест FixedThreadPool
        long fixedTime = testPoolPerformance("FixedThreadPool", () -> {
            ExecutorService pool = Executors.newFixedThreadPool(4);
            return new StandardExecutorWrapper(pool);
        }, TASK_COUNT, TASK_DURATION);
        
        System.out.println("\nРезультаты производительности:");
        System.out.println("CustomThreadPool:    " + customTime + " мс");
        System.out.println("ThreadPoolExecutor:  " + standardTime + " мс");
        System.out.println("FixedThreadPool:     " + fixedTime + " мс");
        
        double customVsStandard = (double) customTime / standardTime;
        System.out.printf("CustomThreadPool vs ThreadPoolExecutor: %.2fx\n", customVsStandard);
    }
    
    /**
     * Анализ влияния различных параметров на производительность
     */
    private static void parameterAnalysis() throws InterruptedException {
        final int TASK_COUNT = 500;
        final int TASK_DURATION = 20;
        
        System.out.println("Анализ влияния параметров:");
        
        // Тест разных размеров ядра
        System.out.println("\n--- Влияние corePoolSize ---");
        for (int coreSize : new int[]{1, 2, 4, 8}) {
            long time = testPoolPerformance("Core=" + coreSize, () -> {
                CustomThreadPool pool = new CustomThreadPool(coreSize, coreSize * 2, 5, TimeUnit.SECONDS, 50, 1);
                return new CustomExecutorWrapper(pool);
            }, TASK_COUNT, TASK_DURATION);
            System.out.println("corePoolSize=" + coreSize + ": " + time + " мс");
        }
        
        // Тест разных размеров очереди  
        System.out.println("\n--- Влияние queueSize ---");
        for (int queueSize : new int[]{10, 25, 50, 100}) {
            long time = testPoolPerformance("Queue=" + queueSize, () -> {
                CustomThreadPool pool = new CustomThreadPool(4, 8, 5, TimeUnit.SECONDS, queueSize, 1);
                return new CustomExecutorWrapper(pool);
            }, TASK_COUNT, TASK_DURATION);
            System.out.println("queueSize=" + queueSize + ": " + time + " мс");
        }
        
        // Тест разных значений minSpareThreads
        System.out.println("\n--- Влияние minSpareThreads ---");
        for (int minSpare : new int[]{0, 1, 2, 4}) {
            long time = testPoolPerformance("MinSpare=" + minSpare, () -> {
                CustomThreadPool pool = new CustomThreadPool(4, 8, 5, TimeUnit.SECONDS, 50, minSpare);
                return new CustomExecutorWrapper(pool);
            }, TASK_COUNT, TASK_DURATION);
            System.out.println("minSpareThreads=" + minSpare + ": " + time + " мс");
        }
    }
    
    /**
     * Тестирование производительности пула
     */
    private static long testPoolPerformance(String poolName, PoolFactory factory, 
                                          int taskCount, int taskDurationMs) throws InterruptedException {
        ExecutorWrapper executor = factory.create();
        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicInteger errors = new AtomicInteger(0);
        
        long startTime = System.currentTimeMillis();
        
        // Отправляем задачи
        for (int i = 0; i < taskCount; i++) {
            try {
                executor.execute(() -> {
                    try {
                        Thread.sleep(taskDurationMs);
                        latch.countDown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        errors.incrementAndGet();
                        latch.countDown();
                    }
                });
            } catch (RejectedExecutionException e) {
                errors.incrementAndGet();
                latch.countDown();
            }
        }
        
        // Ждем завершения всех задач
        latch.await();
        long endTime = System.currentTimeMillis();
        
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        long totalTime = endTime - startTime;
        if (errors.get() > 0) {
            System.out.println(poolName + " - ошибок: " + errors.get());
        }
        
        return totalTime;
    }
    
    /**
     * Тестовая задача с настраиваемой продолжительностью
     */
    private static class TestTask implements Runnable {
        private final String name;
        private final long sleepTime;
        
        public TestTask(String name, long sleepTime) {
            this.name = name;
            this.sleepTime = sleepTime;
        }
        
        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try {
                System.out.println("[" + Thread.currentThread().getName() + "] Начало выполнения: " + name);
                Thread.sleep(sleepTime);
                long duration = System.currentTimeMillis() - startTime;
                System.out.println("[" + Thread.currentThread().getName() + "] Завершено: " + name + " за " + duration + "мс");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.out.println("[" + Thread.currentThread().getName() + "] Прервано: " + name);
            }
        }
        
        @Override
        public String toString() {
            return name;
        }
    }
    
    // Вспомогательные интерфейсы и классы для тестирования
    @FunctionalInterface
    private interface PoolFactory {
        ExecutorWrapper create();
    }
    
    private interface ExecutorWrapper {
        void execute(Runnable task);
        void shutdown();
        boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
    }
    
    private static class StandardExecutorWrapper implements ExecutorWrapper {
        private final ExecutorService executor;
        
        public StandardExecutorWrapper(ExecutorService executor) {
            this.executor = executor;
        }
        
        @Override
        public void execute(Runnable task) {
            executor.execute(task);
        }
        
        @Override
        public void shutdown() {
            executor.shutdown();
        }
        
        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return executor.awaitTermination(timeout, unit);
        }
    }
    
    private static class CustomExecutorWrapper implements ExecutorWrapper {
        private final CustomThreadPool pool;
        
        public CustomExecutorWrapper(CustomThreadPool pool) {
            this.pool = pool;
        }
        
        @Override
        public void execute(Runnable task) {
            pool.execute(task);
        }
        
        @Override
        public void shutdown() {
            pool.shutdown();
        }
        
        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return pool.awaitTermination(timeout, unit);
        }
    }
}
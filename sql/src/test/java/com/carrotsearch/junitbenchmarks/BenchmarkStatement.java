package com.carrotsearch.junitbenchmarks;

import static com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties.BENCHMARK_ROUNDS_PROPERTY;
import static com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties.CONCURRENCY_PROPERTY;
import static com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties.IGNORE_ANNOTATION_OPTIONS_PROPERTY;
import static com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties.IGNORE_CALLGC_PROPERTY;
import static com.carrotsearch.junitbenchmarks.BenchmarkOptionsSystemProperties.WARMUP_ROUNDS_PROPERTY;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Benchmark evaluator statement.
 */
final class BenchmarkStatement extends Statement
{
    private static class LongHolder {
        public long value;
        
        public long getAndSet(long newValue) {
            long tmp = value;
            value = newValue;
            return tmp;
        }
    }

    final static ThreadMXBean threadMXBean;
    final static boolean supportsThreadContention;
    static {
        threadMXBean = ManagementFactory.getThreadMXBean();
        supportsThreadContention = threadMXBean.isThreadContentionMonitoringSupported();
        if (supportsThreadContention) {
            threadMXBean.setThreadContentionMonitoringEnabled(true);
        }
    }

    /**
     * Factored out as a nested class as it needs to keep some data during test
     * evaluation.
     */
    private abstract class BaseEvaluator
    {
        final protected ArrayList<SingleResult> results;

        final protected int warmupRounds;
        final protected int benchmarkRounds;
        final protected int totalRounds;

        final protected Clock clock;

        protected long warmupTime;
        protected long benchmarkTime;
        
        private final ThreadLocal<LongHolder> previousThreadBlockedTime = new ThreadLocal<LongHolder>() {
            protected LongHolder initialValue() {
                return new LongHolder();
            }
        };

        protected BaseEvaluator(int warmupRounds, int benchmarkRounds, int totalRounds, Clock clock)
        {
            super();
            this.warmupRounds = warmupRounds;
            this.benchmarkRounds = benchmarkRounds;
            this.totalRounds = totalRounds;
            this.clock = clock;
            this.results = new ArrayList<SingleResult>(totalRounds);
        }

        protected GCSnapshot gcSnapshot = null;

        protected abstract Result evaluate() throws Throwable;

        protected final SingleResult evaluateInternally(int round) throws Throwable
        {
            // We assume no reordering will take place here.
            final long startTime = clock.time();
            cleanupMemory();
            final long afterGC = clock.time();

            if (round == warmupRounds)
            {
                gcSnapshot = new GCSnapshot();
                benchmarkTime = clock.time();
                warmupTime = benchmarkTime - warmupTime;
            }

            base.evaluate();
            final long endTime = clock.time();

            final long roundBlockedTime;
            if (supportsThreadContention) {
                final long threadId = Thread.currentThread().getId();
                final ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId);
                final long threadBlockedTime = threadInfo.getBlockedTime();
                final long previousValue = previousThreadBlockedTime.get().getAndSet(threadBlockedTime);
                roundBlockedTime = threadBlockedTime - previousValue;
            } else {
                roundBlockedTime = 0;
            }

            return new SingleResult(startTime, afterGC, endTime, roundBlockedTime);
        }

        protected Result computeResult()
        {
            final Statistics stats = Statistics.from(
                results.subList(warmupRounds, totalRounds));

            return new Result(description, benchmarkRounds, warmupRounds, warmupTime,
                benchmarkTime, stats.evaluation, stats.blocked, stats.gc, gcSnapshot, 1);
        }
    }

    /**
     * Performs test method evaluation sequentially.
     */
    private final class SequentialEvaluator extends BaseEvaluator
    {
        SequentialEvaluator(int warmupRounds, int benchmarkRounds, int totalRounds, Clock clock)
        {
            super(warmupRounds, benchmarkRounds, totalRounds, clock);
        }

        @Override
        public Result evaluate() throws Throwable
        {
            warmupTime = clock.time();
            benchmarkTime = 0;
            for (int i = 0; i < totalRounds; i++)
            {
                results.add(evaluateInternally(i));
            }
            benchmarkTime = clock.time() - benchmarkTime;

            return computeResult();
        }
    }

    /**
     * Performs test method evaluation concurrently. The basic idea is to obtain a
     * {@link java.util.concurrent.ThreadPoolExecutor} instance (either new one on each evaluation as it is
     * implemented now or a shared one to avoid excessive thread allocation), wrap it into
     * a <tt>CompletionService&lt;SingleResult&gt;</tt>, pause its execution until the
     * associated task queue is filled with <tt>totalRounds</tt> number of
     * <tt>EvaluatorCallable&lt;SingleResult&gt;</tt>.
     */
    private final class ConcurrentEvaluator extends BaseEvaluator
    {
        private final class EvaluatorCallable implements Callable<SingleResult>
        {
            // Sequence number in order to keep track of warmup / benchmark phase
            private final int i;

            public EvaluatorCallable(int i)
            {
                this.i = i;
            }

            @Override
            public SingleResult call() throws Exception
            {
                latch.await();
                try {
                    return evaluateInternally(i);
                } catch (Exception e) {
                    throw e;
                } catch (Throwable t) {
                    throw new InvocationTargetException(t);
                }
            }
        }

        private final int concurrency;
        private final CountDownLatch latch;


        ConcurrentEvaluator(int warmupRounds, int benchmarkRounds, int totalRounds,
                            int concurrency, Clock clock)
        {
            super(warmupRounds, benchmarkRounds, totalRounds, clock);

            this.concurrency = concurrency;
            this.latch = new CountDownLatch(1);
        }

        /**
         * Perform ThreadPoolExecution initialization. Returns new preconfigured
         * threadPoolExecutor for particular concurrency level and totalRounds to be
         * executed Candidate for further development to mitigate the problem of excessive
         * thread pool creation/destruction.
         *
         * @param concurrency
         * @param totalRounds
         */
        private final ExecutorService getExecutor(int concurrency, int totalRounds)
        {
            return new ThreadPoolExecutor(concurrency, concurrency, 10000,
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(totalRounds));
        }

        /**
         * Perform proper ThreadPool cleanup.
         */
        private final void cleanupExecutor(ExecutorService executor)
        {
            @SuppressWarnings("unused")
            List<Runnable> pending = executor.shutdownNow();
            // Can pending.size() be > 0?
        }

        @Override
        public Result evaluate() throws Throwable
        {
            // Obtain ThreadPoolExecutor (new instance on each test method for now)
            ExecutorService executor = getExecutor(concurrency, totalRounds);
            CompletionService<SingleResult> completed = new ExecutorCompletionService<SingleResult>(
                executor);

            for (int i = 0; i < totalRounds; i++)
            {
                completed.submit(new EvaluatorCallable(i));
            }

            // Allow all the evaluators to proceed to the warmup phase.
            latch.countDown();

            warmupTime = clock.time();
            benchmarkTime = 0;
            try
            {
                for (int i = 0; i < totalRounds; i++)
                {
                    results.add(completed.take().get());
                }

                benchmarkTime = clock.time() - benchmarkTime;
                return computeResult();
            }
            catch (ExecutionException e)
            {
                // Unwrap the Throwable thrown by the tested method.
                e.printStackTrace();
                throw e.getCause().getCause();
            }
            finally
            {
                // Assure proper executor cleanup either on test failure or an successful completion
                cleanupExecutor(executor);
            }
        }

        @Override
        protected Result computeResult()
        {
            Result r = super.computeResult();
            r.concurrency = this.concurrency;
            return r;
        }
    }

    /**
     * How many warmup runs should we execute for each test method?
     */
    final static int DEFAULT_WARMUP_ROUNDS = 5;

    /**
     * How many actual benchmark runs should we execute for each test method?
     */
    final static int DEFAULT_BENCHMARK_ROUNDS = 10;

    /**
     * If <code>true</code>, the local overrides using {@link com.carrotsearch.junitbenchmarks.BenchmarkOptions} are
     * ignored and defaults (or globals passed via system properties) are used.
     */
    private boolean ignoreAnnotationOptions = Boolean
        .getBoolean(IGNORE_ANNOTATION_OPTIONS_PROPERTY);

    /**
     * Disable all forced garbage collector calls.
     */
    private boolean ignoreCallGC = Boolean.getBoolean(IGNORE_CALLGC_PROPERTY);

    private final Description description;
    private final BenchmarkOptions options;
    private final IResultsConsumer [] consumers;

    private final Statement base;


    /* */
    public BenchmarkStatement(Statement base, Description description,
            IResultsConsumer[] consumers) {
        this.base = base;
        this.description = description;
        this.consumers = consumers;

        this.options = resolveOptions(description);
    }

    /* Provide the default options from the annotation. */
    @BenchmarkOptions
    private void defaultOptions()
    {
    }

    /* */
    private BenchmarkOptions resolveOptions(Description description) {
        // Method-level
        BenchmarkOptions options = description.getAnnotation(BenchmarkOptions.class);
        if (options != null) return options;
        
        // Class-level
        Class<?> clz = description.getTestClass();
        while (clz != null)
        {
            options = clz.getAnnotation(BenchmarkOptions.class);
            if (options != null) return options;

            clz = clz.getSuperclass();
        }

        // Defaults.
        try
        {
            return getClass()
                .getDeclaredMethod("defaultOptions")
                .getAnnotation(BenchmarkOptions.class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /* */
    @Override
    public void evaluate() throws Throwable
    {
        final int warmupRounds = getIntOption(options.warmupRounds(),
            WARMUP_ROUNDS_PROPERTY, DEFAULT_WARMUP_ROUNDS);

        final int benchmarkRounds = getIntOption(options.benchmarkRounds(),
            BENCHMARK_ROUNDS_PROPERTY, DEFAULT_BENCHMARK_ROUNDS);

        final int concurrency = getIntOption(options.concurrency(), CONCURRENCY_PROPERTY,
            BenchmarkOptions.CONCURRENCY_SEQUENTIAL);

        final int totalRounds = warmupRounds + benchmarkRounds;

        final BaseEvaluator evaluator;
        if (concurrency == BenchmarkOptions.CONCURRENCY_SEQUENTIAL)
        {
            evaluator = new SequentialEvaluator(warmupRounds, benchmarkRounds, totalRounds, options.clock());
        }
        else
        {
            /*
             * Just don't allow call GC during concurrent execution.
             */
            if (options.callgc())
                throw new IllegalArgumentException("Concurrent benchmark execution must be"
                    + " combined ignoregc=\"true\".");

            int threads = (concurrency == BenchmarkOptions.CONCURRENCY_AVAILABLE_CORES
                    ? Runtime.getRuntime().availableProcessors()
                    : concurrency);

            evaluator = new ConcurrentEvaluator(
                warmupRounds, benchmarkRounds, totalRounds, threads, options.clock());
        }

        final Result result = evaluator.evaluate();

        for (IResultsConsumer consumer : consumers)
            consumer.accept(result);
    }

    /**
     * Best effort attempt to clean up the memory if {@link com.carrotsearch.junitbenchmarks.BenchmarkOptions#callgc()} is
     * enabled.
     */
    private void cleanupMemory()
    {
        if (ignoreCallGC) return;
        if (!options.callgc()) return;

        /*
         * Best-effort GC invocation. I really don't know of any other way to ensure a GC
         * pass.
         */
        System.gc();
        System.gc();
        Thread.yield();
    }

    /**
     * Get an integer override from system properties.
     */
    private int getIntOption(int localValue, String property, int defaultValue)
    {
        final String v = System.getProperty(property);
        if (v != null && v.trim().length() > 0)
        {
            defaultValue = Integer.parseInt(v);
        }

        if (ignoreAnnotationOptions || localValue < 0)
        {
            return defaultValue;
        }

        return localValue;
    }
}
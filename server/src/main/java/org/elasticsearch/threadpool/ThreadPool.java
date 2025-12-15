/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.threadpool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.node.Node;
import org.jspecify.annotations.Nullable;

import io.crate.common.unit.TimeValue;

public class ThreadPool implements Scheduler {

    private static final Logger LOGGER = LogManager.getLogger(ThreadPool.class);

    public static class Names {
        public static final String SAME = "same";
        public static final String GENERIC = "generic";
        public static final String LISTENER = "listener";
        public static final String WRITE = "write";
        public static final String SEARCH = "search";
        public static final String MANAGEMENT = "management";
        public static final String FLUSH = "flush";
        public static final String REFRESH = "refresh";
        public static final String WARMER = "warmer";
        public static final String SNAPSHOT = "snapshot";
        public static final String FORCE_MERGE = "force_merge";
        public static final String FETCH_SHARD_STARTED = "fetch_shard_started";
        public static final String FETCH_SHARD_STORE = "fetch_shard_store";
        public static final String LOGICAL_REPLICATION = "logical_replication";
    }

    public enum ThreadPoolType {
        DIRECT("direct"),
        FIXED("fixed"),
        SCALING("scaling");

        private final String type;

        public String getType() {
            return type;
        }

        ThreadPoolType(String type) {
            this.type = type;
        }

        private static final Map<String, ThreadPoolType> TYPE_MAP;

        static {
            Map<String, ThreadPoolType> typeMap = new HashMap<>();
            for (ThreadPoolType threadPoolType : ThreadPoolType.values()) {
                typeMap.put(threadPoolType.getType(), threadPoolType);
            }
            TYPE_MAP = Collections.unmodifiableMap(typeMap);
        }

        public static ThreadPoolType fromType(String type) {
            ThreadPoolType threadPoolType = TYPE_MAP.get(type);
            if (threadPoolType == null) {
                throw new IllegalArgumentException("no ThreadPoolType for " + type);
            }
            return threadPoolType;
        }
    }

    private final Map<String, ExecutorHolder> executors;

    private final CachedTimeThread cachedTimeThread;

    private final List<ExecutorBuilder> builders;

    private final ScheduledThreadPoolExecutor scheduler;

    public Collection<ExecutorBuilder> builders() {
        return builders;
    }

    public static Setting<TimeValue> ESTIMATED_TIME_INTERVAL_SETTING =
        Setting.timeSetting("thread_pool.estimated_time_interval",
            TimeValue.timeValueMillis(200), TimeValue.ZERO, Setting.Property.NodeScope);

    public ThreadPool(final Settings settings) {
        assert Node.NODE_NAME_SETTING.exists(settings);

        final int availableProcessors = EsExecutors.numberOfProcessors(settings);
        final int halfProcMaxAt5 = halfNumberOfProcessorsMaxFive(availableProcessors);
        final int halfProcMaxAt10 = halfNumberOfProcessorsMaxTen(availableProcessors);
        final int genericThreadPoolMax = boundedBy(4 * availableProcessors, 128, 512);

        builders = List.of(
            new ScalingExecutorBuilder(Names.GENERIC, 4, genericThreadPoolMax, TimeValue.timeValueSeconds(30)),
            new FixedExecutorBuilder(settings, Names.WRITE, availableProcessors, 200),
            new PrioFixedExecutorBuilder(settings, Names.SEARCH, searchThreadPoolSize(availableProcessors), 1000),
            new ScalingExecutorBuilder(Names.MANAGEMENT, 1, 5, TimeValue.timeValueMinutes(5)),
            // no queue athis means clients will need to handle rejections on listener queue even if the operation succeeded
            // the assumpon here is that the listeners should be very lightweight on the listeners side
            new FixedExecutorBuilder(settings, Names.LISTENER, halfProcMaxAt10, -1),
            new ScalingExecutorBuilder(Names.FLUSH, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)),
            new ScalingExecutorBuilder(Names.REFRESH, 1, halfProcMaxAt10, TimeValue.timeValueMinutes(5)),
            new ScalingExecutorBuilder(Names.WARMER, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)),
            new ScalingExecutorBuilder(Names.SNAPSHOT, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)),
            new ScalingExecutorBuilder(Names.FETCH_SHARD_STARTED, 1, 2 * availableProcessors, TimeValue.timeValueMinutes(5)),
            new FixedExecutorBuilder(settings, Names.FORCE_MERGE, oneEightOfProcessors(availableProcessors), -1),
            new ScalingExecutorBuilder(Names.FETCH_SHARD_STORE, 1, 2 * availableProcessors, TimeValue.timeValueMinutes(5)),
            new FixedExecutorBuilder(settings, Names.LOGICAL_REPLICATION, searchThreadPoolSize(availableProcessors), 100)
        );
        HashMap<String, ExecutorHolder> executors = HashMap.newHashMap(builders.size() + 1);
        for (ExecutorBuilder builder : builders) {
            final ExecutorHolder executorHolder = builder.build(settings);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("created thread pool: {}", builder.formatInfo(executorHolder.info));
            }
            ExecutorHolder previous = executors.put(executorHolder.info.name(), executorHolder);
            if (previous != null) {
                throw new IllegalStateException("duplicate executors with name [" + executorHolder.info.name() + "] registered");
            }
        }
        executors.put(Names.SAME, new ExecutorHolder(EsExecutors.directExecutor(), new Info(Names.SAME, ThreadPoolType.DIRECT)));
        this.executors = Map.copyOf(executors);

        this.scheduler = Scheduler.initScheduler(settings);
        TimeValue estimatedTimeInterval = ESTIMATED_TIME_INTERVAL_SETTING.get(settings);
        this.cachedTimeThread = new CachedTimeThread(EsExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.cachedTimeThread.start();
    }

    /**
     * Returns a value of milliseconds that may be used for relative time calculations.
     * <p/>
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long relativeTimeInMillis() {
        return cachedTimeThread.relativeTimeInMillis();
    }

    /**
     * Returns the value of milliseconds since UNIX epoch.
     * <p/>
     * This method should only be used for exact date/time formatting. For calculating
     * time deltas that should not suffer from negative deltas, which are possible with
     * this method, see {@link #relativeTimeInMillis()}.
     */
    public long absoluteTimeInMillis() {
        return cachedTimeThread.absoluteTimeInMillis();
    }

    public ThreadPoolStats stats() {
        ArrayList<ThreadPoolStats.Stats> stats = new ArrayList<>(executors.size() - 1); // "same" is excluded
        for (ExecutorHolder holder : executors.values()) {

            final String name = holder.info.name();
            // no need to have info on "same" thread pool
            if ("same".equals(name)) {
                continue;
            }
            ThreadPoolStats.Stats poolStats = stats(name);
            if (poolStats != null) {
                stats.add(poolStats);
            }
        }
        return new ThreadPoolStats(stats);
    }

    public ThreadPoolStats.@Nullable Stats stats(String name) {
        ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            return null;
        }
        if (holder.executor() instanceof ThreadPoolExecutor threadPoolExecutor) {
            return new ThreadPoolStats.Stats(
                name,
                threadPoolExecutor.getPoolSize(),
                threadPoolExecutor.getQueue().size(),
                threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getRejectedExecutionHandler() instanceof XRejectedExecutionHandler rejectedHandler
                    ? rejectedHandler.rejected()
                    : -1,
                threadPoolExecutor.getLargestPoolSize(),
                threadPoolExecutor.getCompletedTaskCount()
            );
        }
        return new ThreadPoolStats.Stats(name, -1, -1, -1, -1, -1, -1);
    }

    public ThreadPool.@Nullable Info info(String name) {
        ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            return null;
        }
        return holder.info();
    }

    /**
     * Get the generic {@link ExecutorService}. This executor service
     * {@link Executor#execute(Runnable)} method will run the {@link Runnable} it is given in the
     * {@link ThreadContext} of the thread that queues it.
     * <p>
     * Warning: this {@linkplain ExecutorService} will not throw {@link RejectedExecutionException}
     * if you submit a task while it shutdown. It will instead silently queue it and not run it.
     */
    public ExecutorService generic() {
        return (ExecutorService) executor(Names.GENERIC);
    }

    /**
     * Get the {@link ExecutorService} with the given name. This executor service's
     * {@link Executor#execute(Runnable)} method will run the {@link Runnable} it is given in the
     * {@link ThreadContext} of the thread that queues it.
     * <p>
     * Warning: this {@linkplain ExecutorService} might not throw {@link RejectedExecutionException}
     * if you submit a task while it shutdown. It will instead silently queue it and not run it.
     *
     * @param name the name of the executor service to obtain
     * @throws IllegalArgumentException if no executor service with the specified name exists
     */
    public Executor executor(String name) {
        final ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            throw new IllegalArgumentException("no executor service found for [" + name + "]");
        }
        return holder.executor();
    }

    /**
     * Schedules a one-shot command to run after a given delay. The command is not run in the context of the calling thread. To preserve the
     * context of the calling thread you may call <code>threadPool.getThreadContext().preserveContext</code> on the runnable before passing
     * it to this method.
     *
     * @param command the command to run
     * @param delay delay before the task executes
     * @param executor the name of the thread pool on which to execute this task. SAME means "execute on the scheduler thread" which changes
     *        the meaning of the ScheduledFuture returned by this method. In that case the ScheduledFuture will complete only when the
     *        command completes.
     * @return a ScheduledFuture who's get will return when the task is has been added to its target thread pool and throw an exception if
     *         the task is canceled before it was added to its target thread pool. Once the task has been added to its target thread pool
     *         the ScheduledFuture will cannot interact with it.
     * @throws org.elasticsearch.common.util.concurrent.EsRejectedExecutionException if the task cannot be scheduled for execution
     */
    @Override
    public ScheduledCancellable schedule(Runnable command, TimeValue delay, String executor) {
        if (!Names.SAME.equals(executor)) {
            command = new ThreadedRunnable(command, executor(executor));
        }
        return new ScheduledCancellableAdapter(scheduler.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));
    }

    public Cancellable scheduleUnlessShuttingDown(TimeValue delay, String executor, Runnable command) {
        try {
            return schedule(command, delay, executor);
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                LOGGER.debug(new ParameterizedMessage("could not schedule execution of [{}] after [{}] on [{}] as executor is shut down",
                    command, delay, executor), e);
                return Cancellable.CANCELLED_NOOP;
            } else {
                throw e;
            }
        }
    }

    @Override
    public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
        return new ReschedulingRunnable(command, interval, executor, this, (e) -> {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(() -> new ParameterizedMessage("scheduled task [{}] was rejected on thread pool [{}]",
                        command, executor), e);
            }
        }, (e) -> LOGGER.warn(() -> new ParameterizedMessage("failed to run scheduled task [{}] on thread pool [{}]",
                command, executor), e));
    }

    protected final void stopCachedTimeThread() {
        cachedTimeThread.running = false;
        cachedTimeThread.interrupt();
    }

    public void shutdown() {
        stopCachedTimeThread();
        scheduler.shutdown();
        for (ExecutorHolder executorHolder : executors.values()) {
            Executor executor = executorHolder.executor();
            if (executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).shutdown();
            }
        }
    }

    public void shutdownNow() {
        stopCachedTimeThread();
        scheduler.shutdownNow();
        for (ExecutorHolder executorHolder : executors.values()) {
            Executor executor = executorHolder.executor();
            if (executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor).shutdownNow();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (ExecutorHolder executorHolder : executors.values()) {
            Executor executor = executorHolder.executor();
            if (executor instanceof ThreadPoolExecutor) {
                result &= ((ThreadPoolExecutor) executor).awaitTermination(timeout, unit);
            }
        }
        cachedTimeThread.join(unit.toMillis(timeout));
        return result;
    }

    public ScheduledExecutorService scheduler() {
        return this.scheduler;
    }

    /**
     * Constrains a value between minimum and maximum values
     * (inclusive).
     *
     * @param value the value to constrain
     * @param min   the minimum acceptable value
     * @param max   the maximum acceptable value
     * @return min if value is less than min, max if value is greater
     * than value, otherwise value
     */
    static int boundedBy(int value, int min, int max) {
        return Math.min(max, Math.max(min, value));
    }

    static int halfNumberOfProcessorsMaxFive(int numberOfProcessors) {
        return boundedBy((numberOfProcessors + 1) / 2, 1, 5);
    }

    static int halfNumberOfProcessorsMaxTen(int numberOfProcessors) {
        return boundedBy((numberOfProcessors + 1) / 2, 1, 10);
    }

    static int oneEightOfProcessors(int numberOfProcessors) {
        return boundedBy(numberOfProcessors / 8, 1, Integer.MAX_VALUE);
    }

    public static int searchThreadPoolSize(int availableProcessors) {
        return ((availableProcessors * 3) / 2) + 1;
    }

    static class ThreadedRunnable implements Runnable {

        private final Runnable runnable;

        private final Executor executor;

        ThreadedRunnable(Runnable runnable, Executor executor) {
            this.runnable = runnable;
            this.executor = executor;
        }

        @Override
        public void run() {
            try {
                executor.execute(runnable);
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    LOGGER.debug(new ParameterizedMessage("could not schedule execution of [{}] on [{}] as executor is shut down",
                        runnable, executor), e);
                } else {
                    throw e;
                }
            }
        }

        @Override
        public int hashCode() {
            return runnable.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ThreadedRunnable && runnable.equals(obj);
        }

        @Override
        public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    /**
     * A thread to cache millisecond time values from
     * {@link System#nanoTime()} and {@link System#currentTimeMillis()}.
     * <p/>
     * The values are updated at a specified interval.
     */
    static class CachedTimeThread extends Thread {

        final long interval;
        volatile boolean running = true;
        volatile long relativeMillis;
        volatile long absoluteMillis;

        CachedTimeThread(String name, long interval) {
            super(name);
            this.interval = interval;
            this.relativeMillis = TimeValue.nsecToMSec(System.nanoTime());
            this.absoluteMillis = System.currentTimeMillis();
            setDaemon(true);
        }

        /**
         * Return the current time used for relative calculations. This is
         * {@link System#nanoTime()} truncated to milliseconds.
         * <p>
         * If {@link ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING} is set to 0
         * then the cache is disabled and the method calls {@link System#nanoTime()}
         * whenever called. Typically used for testing.
         */
        long relativeTimeInMillis() {
            if (0 < interval) {
                return relativeMillis;
            }
            return TimeValue.nsecToMSec(System.nanoTime());
        }

        /**
         * Return the current epoch time, used to find absolute time. This is
         * a cached version of {@link System#currentTimeMillis()}.
         * <p>
         * If {@link ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING} is set to 0
         * then the cache is disabled and the method calls {@link System#currentTimeMillis()}
         * whenever called. Typically used for testing.
         */
        long absoluteTimeInMillis() {
            if (0 < interval) {
                return absoluteMillis;
            }
            return System.currentTimeMillis();
        }

        @Override
        public void run() {
            while (running && 0 < interval) {
                relativeMillis = TimeValue.nsecToMSec(System.nanoTime());
                absoluteMillis = System.currentTimeMillis();
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    running = false;
                    return;
                }
            }
        }
    }

    record ExecutorHolder(Executor executor, Info info) {
        ExecutorHolder {
            assert executor instanceof EsThreadPoolExecutor || executor == EsExecutors.directExecutor()
                : "Executor must either be the DIRECT_EXECUTOR or an instance of EsThreadPoolExecutor";
        }
    }

    public record Info(String name,
                       ThreadPoolType type,
                       int min,
                       int max,
                       @Nullable TimeValue keepAlive,
                       @Nullable SizeValue queueSize) implements Writeable {

        public Info(String name, ThreadPoolType type) {
            this(name, type, -1, -1, null, null);
        }

        public static Info of(StreamInput in) throws IOException {
            String name = in.readString();
            ThreadPoolType type = ThreadPoolType.fromType(in.readString());
            int min = in.readInt();
            int max = in.readInt();
            TimeValue keepAlive = in.readOptionalTimeValue();
            SizeValue queueSize = in.readOptionalWriteable(SizeValue::new);
            return new Info(name, type, min, max, keepAlive, queueSize);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type.getType());
            out.writeInt(min);
            out.writeInt(max);
            out.writeOptionalTimeValue(keepAlive);
            out.writeOptionalWriteable(queueSize);
        }
    }

    /**
     * Returns <code>true</code> if the given service was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ExecutorService service, long timeout, TimeUnit timeUnit) {
        if (service != null) {
            service.shutdown();
            if (awaitTermination(service, timeout, timeUnit)) return true;
            service.shutdownNow();
            return awaitTermination(service, timeout, timeUnit);
        }
        return false;
    }

    private static boolean awaitTermination(
            final ExecutorService service,
            final long timeout,
            final TimeUnit timeUnit) {
        try {
            if (service.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Returns <code>true</code> if the given pool was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ThreadPool pool, long timeout, TimeUnit timeUnit) {
        if (pool != null) {
            // Leverage try-with-resources to close the threadpool
            pool.shutdown();
            if (awaitTermination(pool, timeout, timeUnit)) {
                return true;
            }
            // last resort
            pool.shutdownNow();
            return awaitTermination(pool, timeout, timeUnit);
        }
        return false;
    }

    private static boolean awaitTermination(
            final ThreadPool threadPool,
            final long timeout,
            final TimeUnit timeUnit) {
        try {
            if (threadPool.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }


    public static boolean assertNotScheduleThread(String reason) {
        assert Thread.currentThread().getName().contains("scheduler") == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the scheduler thread. Reason: [" + reason + "]";
        return true;
    }

    public static boolean assertCurrentMethodIsNotCalledRecursively() {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        assert stackTraceElements.length >= 3 : stackTraceElements.length;
        assert stackTraceElements[0].getMethodName().equals("getStackTrace") : stackTraceElements[0];
        assert stackTraceElements[1].getMethodName().equals("assertCurrentMethodIsNotCalledRecursively") : stackTraceElements[1];
        final StackTraceElement testingMethod = stackTraceElements[2];
        for (int i = 3; i < stackTraceElements.length; i++) {
            assert stackTraceElements[i].getClassName().equals(testingMethod.getClassName()) == false
                || stackTraceElements[i].getMethodName().equals(testingMethod.getMethodName()) == false :
                testingMethod.getClassName() + "#" + testingMethod.getMethodName() + " is called recursively";
        }
        return true;
    }
}

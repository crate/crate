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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Scheduler that allows to schedule one-shot and periodic commands.
 */
public interface Scheduler {

    static ScheduledThreadPoolExecutor initScheduler(Settings settings) {
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1,
                EsExecutors.daemonThreadFactory(settings, "scheduler"), new EsAbortPolicy());
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    static boolean terminate(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor, long timeout, TimeUnit timeUnit) {
        scheduledThreadPoolExecutor.shutdown();
        if (awaitTermination(scheduledThreadPoolExecutor, timeout, timeUnit)) {
            return true;
        }
        // last resort
        scheduledThreadPoolExecutor.shutdownNow();
        return awaitTermination(scheduledThreadPoolExecutor, timeout, timeUnit);
    }

    static boolean awaitTermination(final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor,
            final long timeout, final TimeUnit timeUnit) {
        try {
            if (scheduledThreadPoolExecutor.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Does nothing by default but can be used by subclasses to save the current thread context and wraps the command in a Runnable
     * that restores that context before running the command.
     */
    default Runnable preserveContext(Runnable command) {
        return command;
    }

    /**
     * Schedules a one-shot command to be run after a given delay. The command is not run in the context of the calling thread.
     * To preserve the context of the calling thread you may call {@link #preserveContext(Runnable)} on the runnable before passing
     * it to this method.
     * The command runs on scheduler thread. Do not run blocking calls on the scheduler thread. Subclasses may allow
     * to execute on a different executor, in which case blocking calls are allowed.
     *
     * @param delay delay before the task executes
     * @param executor the name of the executor that has to execute this task. Ignored in the default implementation but can be used
     *                 by subclasses that support multiple executors.
     * @param command the command to run
     * @return a ScheduledFuture who's get will return when the task has been added to its target thread pool and throws an exception if
     *         the task is canceled before it was added to its target thread pool. Once the task has been added to its target thread pool
     *         the ScheduledFuture cannot interact with it.
     * @throws EsRejectedExecutionException if the task cannot be scheduled for execution
     */
    ScheduledFuture<?> schedule(TimeValue delay, String executor, Runnable command);

    /**
     * Schedules a periodic action that runs on scheduler thread. Do not run blocking calls on the scheduler thread. Subclasses may allow
     * to execute on a different executor, in which case blocking calls are allowed.
     *
     * @param command the action to take
     * @param interval the delay interval
     * @param executor the name of the executor that has to execute this task. Ignored in the default implementation but can be used
     *                 by subclasses that support multiple executors.
     * @return a {@link Cancellable} that can be used to cancel the subsequent runs of the command. If the command is running, it will
     *         not be interrupted.
     */
    default Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
        return new ReschedulingRunnable(command, interval, executor, this, (e) -> {}, (e) -> {});
    }

    /**
     * This interface represents an object whose execution may be cancelled during runtime.
     */
    interface Cancellable {

        /**
         * Cancel the execution of this object. This method is idempotent.
         */
        void cancel();

        /**
         * Check if the execution has been cancelled
         * @return true if cancelled
         */
        boolean isCancelled();
    }

    /**
     * This class encapsulates the scheduling of a {@link Runnable} that needs to be repeated on a interval. For example, checking a value
     * for cleanup every second could be done by passing in a Runnable that can perform the check and the specified interval between
     * executions of this runnable. <em>NOTE:</em> the runnable is only rescheduled to run again after completion of the runnable.
     *
     * For this class, <i>completion</i> means that the call to {@link Runnable#run()} returned or an exception was thrown and caught. In
     * case of an exception, this class will log the exception and reschedule the runnable for its next execution. This differs from the
     * {@link ScheduledThreadPoolExecutor#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} semantics as an exception there would
     * terminate the rescheduling of the runnable.
     */
    final class ReschedulingRunnable extends AbstractRunnable implements Cancellable {

        private final Runnable runnable;
        private final TimeValue interval;
        private final String executor;
        private final Scheduler scheduler;
        private final Consumer<Exception> rejectionConsumer;
        private final Consumer<Exception> failureConsumer;

        private volatile boolean run = true;

        /**
         * Creates a new rescheduling runnable and schedules the first execution to occur after the interval specified
         *
         * @param runnable the {@link Runnable} that should be executed periodically
         * @param interval the time interval between executions
         * @param executor the executor where this runnable should be scheduled to run
         * @param scheduler the {@link Scheduler} instance to use for scheduling
         */
        ReschedulingRunnable(Runnable runnable, TimeValue interval, String executor, Scheduler scheduler,
                             Consumer<Exception> rejectionConsumer, Consumer<Exception> failureConsumer) {
            this.runnable = runnable;
            this.interval = interval;
            this.executor = executor;
            this.scheduler = scheduler;
            this.rejectionConsumer = rejectionConsumer;
            this.failureConsumer = failureConsumer;
            scheduler.schedule(interval, executor, this);
        }

        @Override
        public void cancel() {
            run = false;
        }

        @Override
        public boolean isCancelled() {
            return run == false;
        }

        @Override
        public void doRun() {
            // always check run here since this may have been cancelled since the last execution and we do not want to run
            if (run) {
                runnable.run();
            }
        }

        @Override
        public void onFailure(Exception e) {
            failureConsumer.accept(e);
        }

        @Override
        public void onRejection(Exception e) {
            run = false;
            rejectionConsumer.accept(e);
        }

        @Override
        public void onAfter() {
            // if this has not been cancelled reschedule it to run again
            if (run) {
                try {
                    scheduler.schedule(interval, executor, this);
                } catch (final EsRejectedExecutionException e) {
                    onRejection(e);
                }
            }
        }
    }
}

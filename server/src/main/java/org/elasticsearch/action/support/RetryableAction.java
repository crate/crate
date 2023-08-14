/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.common.unit.TimeValue;

/**
 * A action that will be retried on failure if {@link RetryableAction#shouldRetry(Exception)} returns true.
 * The executor the action will be executed on can be defined in the constructor. Otherwise, SAME is the
 * default. The action will be retried with exponentially increasing delay periods until the timeout period
 * has been reached.
 */
public abstract class RetryableAction<Response> {

    private final Logger logger;

    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final ThreadPool threadPool;
    private final long initialDelayMillis;
    private final long timeoutMillis;
    private final long startMillis;
    private final ActionListener<Response> finalListener;

    private volatile Scheduler.ScheduledCancellable retryTask;

    public RetryableAction(Logger logger,
                           ThreadPool threadPool,
                           TimeValue initialDelay,
                           TimeValue timeoutValue,
                           ActionListener<Response> listener) {
        this.logger = logger;
        this.threadPool = threadPool;
        this.initialDelayMillis = initialDelay.millis();
        if (initialDelayMillis < 1) {
            throw new IllegalArgumentException("Initial delay was less than 1 millisecond: " + initialDelay);
        }
        this.timeoutMillis = timeoutValue.millis();
        this.startMillis = threadPool.relativeTimeInMillis();
        this.finalListener = listener;
    }

    public void run() {
        final RetryingListener retryingListener = new RetryingListener(initialDelayMillis, null);
        final Runnable runnable = createRunnable(retryingListener);
        runnable.run();
    }

    public void cancel(Exception e) {
        if (isDone.compareAndSet(false, true)) {
            Scheduler.ScheduledCancellable localRetryTask = this.retryTask;
            if (localRetryTask != null) {
                localRetryTask.cancel();
            }
            onFinished();
            finalListener.onFailure(e);
        }
    }

    private Runnable createRunnable(RetryingListener retryingListener) {
        return new ActionRunnable<Response>(retryingListener) {

            @Override
            protected void doRun() {
                retryTask = null;
                // It is possible that the task was cancelled in between the retry being dispatched and now
                if (isDone.get() == false) {
                    tryAction(listener);
                }
            }

            @Override
            public void onRejection(Exception e) {
                retryTask = null;
                // TODO: The only implementations of this class use SAME which means the execution will not be
                //  rejected. Future implementations can adjust this functionality as needed.
                onFailure(e);
            }
        };
    }

    public abstract void tryAction(ActionListener<Response> listener);

    public abstract boolean shouldRetry(Exception e);

    public void onFinished() {
    }

    private class RetryingListener implements ActionListener<Response> {

        private static final int MAX_EXCEPTIONS = 4;

        private final long delayMillisBound;
        private ArrayDeque<Exception> caughtExceptions;

        private RetryingListener(long delayMillisBound, ArrayDeque<Exception> caughtExceptions) {
            this.delayMillisBound = delayMillisBound;
            this.caughtExceptions = caughtExceptions;
        }

        @Override
        public void onResponse(Response response) {
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (shouldRetry(e)) {
                final long elapsedMillis = threadPool.relativeTimeInMillis() - startMillis;
                if (elapsedMillis >= timeoutMillis) {
                    logger.debug(() -> new ParameterizedMessage("retryable action timed out after {}",
                        TimeValue.timeValueMillis(elapsedMillis)), e);
                    onFinalFailure(e);
                } else {
                    addException(e);

                    final long nextDelayMillisBound = Math.min(delayMillisBound * 2, Integer.MAX_VALUE);
                    final RetryingListener retryingListener = new RetryingListener(nextDelayMillisBound, caughtExceptions);
                    final Runnable runnable = createRunnable(retryingListener);
                    final long delayMillis = Randomness.get().nextInt(Math.toIntExact(delayMillisBound)) + 1;
                    if (isDone.get() == false) {
                        final TimeValue delay = TimeValue.timeValueMillis(delayMillis);
                        logger.debug(() -> new ParameterizedMessage("retrying action that failed in {}", delay), e);
                        try {
                            retryTask = threadPool.schedule(runnable, delay, ThreadPool.Names.SAME);
                        } catch (EsRejectedExecutionException ree) {
                            onFinalFailure(ree);
                        }
                    }
                }
            } else {
                onFinalFailure(e);
            }
        }

        private void onFinalFailure(Exception e) {
            addException(e);
            if (isDone.compareAndSet(false, true)) {
                onFinished();
                finalListener.onFailure(buildFinalException());
            }
        }

        private Exception buildFinalException() {
            final Exception topLevel = caughtExceptions.removeFirst();
            Exception suppressed;
            while ((suppressed = caughtExceptions.pollFirst()) != null) {
                topLevel.addSuppressed(suppressed);
            }
            return topLevel;
        }

        private void addException(Exception e) {
            if (caughtExceptions != null) {
                if (caughtExceptions.size() == MAX_EXCEPTIONS) {
                    caughtExceptions.removeLast();
                }
            } else {
                caughtExceptions = new ArrayDeque<>(MAX_EXCEPTIONS);
            }
            caughtExceptions.addFirst(e);
        }
    }
}

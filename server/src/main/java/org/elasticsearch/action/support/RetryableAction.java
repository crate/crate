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

import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import io.crate.common.collections.RingBuffer;
import io.crate.common.exceptions.Exceptions;
import io.crate.common.unit.TimeValue;
import io.crate.exceptions.SQLExceptions;

/**
 * A action that will be retried on failure if {@link RetryableAction#shouldRetry(Exception)} returns true.
 * The executor the action will be executed on can be defined in the constructor. Otherwise, SAME is the
 * default. The action will be retried with exponentially increasing delay periods until the timeout period
 * has been reached.
 */
public abstract class RetryableAction<Response> {

    private static final Logger LOGGER = LogManager.getLogger(RetryableAction.class);

    private final Logger logger;

    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler;
    private final ActionListener<Response> finalListener;
    private final Iterator<TimeValue> delay;
    private final ActionRunnable<Response> runnable;

    private volatile ScheduledFuture<?> retryTask;


    public static <T> RetryableAction<T> of(ScheduledExecutorService scheduler,
                                            Consumer<ActionListener<T>> command,
                                            Iterable<TimeValue> backoffPolicy,
                                            ActionListener<T> listener) {
        return new RetryableAction<T>(LOGGER, scheduler, backoffPolicy, listener) {

            @Override
            public void tryAction(ActionListener<T> listener) {
                command.accept(listener);
            }
        };
    }

    public RetryableAction(Logger logger,
                           ScheduledExecutorService scheduler,
                           Iterable<TimeValue> backoffPolicy,
                           ActionListener<Response> listener) {
        this.logger = logger;
        this.scheduler = scheduler;
        this.delay = backoffPolicy.iterator();
        this.finalListener = listener;
        this.runnable = new ActionRunnable<Response>(new RetryingListener()) {

            @Override
            public void doRun() throws Exception {
                retryTask = null;
                // It is possible that the task was cancelled in between the retry being dispatched and now
                if (isDone.get() == false) {
                    tryAction(this.listener);
                }
            }

            @Override
            public void onRejection(Exception e) {
                retryTask = null;
                onFailure(e);
            }
        };
    }

    public void run() {
        runnable.run();
    }

    public void cancel(Exception e) {
        if (isDone.compareAndSet(false, true)) {
            ScheduledFuture<?> localRetryTask = this.retryTask;
            if (localRetryTask != null) {
                localRetryTask.cancel(false);
            }
            onFinished();
            finalListener.onFailure(e);
        }
    }

    public abstract void tryAction(ActionListener<Response> listener);

    public boolean shouldRetry(Throwable t) {
        return t instanceof EsRejectedExecutionException rejected && !rejected.isExecutorShutdown();
    }

    public void onFinished() {
    }

    private class RetryingListener implements ActionListener<Response> {

        private static final int MAX_EXCEPTIONS = 4;

        private RingBuffer<Exception> caughtExceptions;

        private RetryingListener() {
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
            Throwable t = SQLExceptions.unwrap(e);
            if (shouldRetry(t) && delay.hasNext()) {
                TimeValue currentDelay = delay.next();
                addException(e);
                if (isDone.get() == false) {
                    logger.error("Retrying action that failed in delay={} err={}", currentDelay, e);
                    try {
                        retryTask = scheduler.schedule(runnable, currentDelay.millis(), TimeUnit.MILLISECONDS);
                    } catch (EsRejectedExecutionException ree) {
                        onFinalFailure(ree);
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
                finalListener.onFailure(Exceptions.merge(caughtExceptions));
            }
        }

        private void addException(Exception e) {
            if (caughtExceptions == null) {
                caughtExceptions = new RingBuffer<>(MAX_EXCEPTIONS);
            }
            caughtExceptions.add(e);
        }
    }
}

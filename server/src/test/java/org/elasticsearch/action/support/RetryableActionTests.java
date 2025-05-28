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

import static io.crate.common.unit.TimeValue.timeValueMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.common.unit.TimeValue;
import io.crate.concurrent.FutureActionListener;

public class RetryableActionTests extends ESTestCase {

    private DeterministicTaskQueue taskQueue;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        taskQueue = new DeterministicTaskQueue(settings, random());
    }

    @Test
    public void testRetryableActionNoRetries() throws Exception {
        final AtomicInteger executedCount = new AtomicInteger();
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(
                logger,
                taskQueue.getThreadPool().scheduler(),
                BackoffPolicy.exponentialBackoff(timeValueMillis(10), TimeValue.timeValueSeconds(30)),
                future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                listener.onResponse(true);
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                return true;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertThat(executedCount.get()).isEqualTo(1);
        assertThat(future.get()).isTrue();
    }

    @Test
    public void testRetryableActionWillRetry() throws Exception {
        int expectedRetryCount = randomIntBetween(1, 8);
        final AtomicInteger remainingFailedCount = new AtomicInteger(expectedRetryCount);
        final AtomicInteger retryCount = new AtomicInteger();
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        TimeValue initialDelay = TimeValue.timeValueMillis(10);
        TimeValue timeout = TimeValue.timeValueSeconds(30);
        Iterable<TimeValue> backoffPolicy = BackoffPolicy.exponentialBackoff(initialDelay, timeout);
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(
                logger,
                taskQueue.getThreadPool().scheduler(),
                backoffPolicy,
                future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (remainingFailedCount.getAndDecrement() == 0) {
                    listener.onResponse(true);
                } else {
                    if (randomBoolean()) {
                        listener.onFailure(new EsRejectedExecutionException("", false));
                    } else {
                        throw new EsRejectedExecutionException("", false);
                    }
                }
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                retryCount.getAndIncrement();
                return e instanceof EsRejectedExecutionException;
            }
        };
        Iterator<TimeValue> expectedDelays = backoffPolicy.iterator();
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        for (int i = 0; i < expectedRetryCount; ++i) {
            assertThat(taskQueue.hasDeferredTasks()).isTrue();
            TimeValue expectedDelay = expectedDelays.next();
            final long deferredExecutionTime = taskQueue.getLatestDeferredExecutionTime();
            assertThat(deferredExecutionTime).isLessThanOrEqualTo(expectedDelay.millis() + previousDeferredTime);
            previousDeferredTime = deferredExecutionTime;
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertThat(retryCount.get()).isEqualTo(expectedRetryCount);
        assertThat(future.get()).isTrue();
    }

    @Test
    public void testRetryableActionTimeout() {
        final AtomicInteger retryCount = new AtomicInteger();
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(
                logger,
                taskQueue.getThreadPool().scheduler(),
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(1)),
                future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (randomBoolean()) {
                    listener.onFailure(new EsRejectedExecutionException("", false));
                } else {
                    throw new EsRejectedExecutionException("", false);
                }
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                retryCount.getAndIncrement();
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        long previousDeferredTime = 0;
        while (previousDeferredTime < 1000) {
            assertThat(taskQueue.hasDeferredTasks()).isTrue();
            previousDeferredTime = taskQueue.getLatestDeferredExecutionTime();
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }

        assertThat(taskQueue.hasDeferredTasks()).isFalse();
        assertThat(taskQueue.hasRunnableTasks()).isFalse();

        assertThatThrownBy(future::get)
            .hasCauseExactlyInstanceOf(EsRejectedExecutionException.class);
    }

    @Test
    public void testTimeoutOfZeroMeansNoRetry() {
        final AtomicInteger executedCount = new AtomicInteger();
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(
                logger,
                taskQueue.getThreadPool().scheduler(),
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(0)),
                future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                throw new EsRejectedExecutionException("rejected", false);
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertThat(executedCount.get()).isEqualTo(1);
        assertThatThrownBy(future::get)
            .hasCauseExactlyInstanceOf(EsRejectedExecutionException.class);
    }

    @Test
    public void testFailedBecauseNotRetryable() {
        final AtomicInteger executedCount = new AtomicInteger();
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(
                logger,
                taskQueue.getThreadPool().scheduler(),
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(30)),
                future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                executedCount.getAndIncrement();
                throw new IllegalStateException();
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();

        assertThat(executedCount.get()).isEqualTo(1);
        assertThatThrownBy(future::get).hasCauseExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRetryableActionCancelled() {
        final AtomicInteger executedCount = new AtomicInteger();
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        final RetryableAction<Boolean> retryableAction = new RetryableAction<Boolean>(
                logger,
                taskQueue.getThreadPool().scheduler(),
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(10), TimeValue.timeValueSeconds(30)),
                future) {

            @Override
            public void tryAction(ActionListener<Boolean> listener) {
                if (executedCount.incrementAndGet() == 1) {
                    throw new EsRejectedExecutionException("", false);
                } else {
                    listener.onResponse(true);
                }
            }

            @Override
            public boolean shouldRetry(Throwable e) {
                return e instanceof EsRejectedExecutionException;
            }
        };
        retryableAction.run();
        taskQueue.runAllRunnableTasks();
        assertThat(taskQueue.hasDeferredTasks()).isTrue();
        taskQueue.advanceTime();

        retryableAction.cancel(new ElasticsearchException("Cancelled"));
        taskQueue.runAllRunnableTasks();

        // A second run will not occur because it is cancelled
        assertThat(executedCount.get()).isEqualTo(1);
        assertThatThrownBy(future::get).hasCauseExactlyInstanceOf(ElasticsearchException.class);
    }
}

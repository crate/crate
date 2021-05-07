/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect.stats;

import io.crate.user.User;
import io.crate.breaker.SizeEstimator;
import io.crate.common.collections.BlockingEvictingQueue;
import io.crate.expression.reference.sys.job.ContextLog;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.planner.Plan;
import io.crate.planner.operators.StatementClassifier;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.common.breaker.CircuitBreaker;
import io.crate.common.unit.TimeValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RamAccountingQueueSinkTest extends ESTestCase {

    private static final NoopLogEstimator NOOP_ESTIMATOR = new NoopLogEstimator();

    private ScheduledExecutorService scheduler;
    private QueueSink logSink;

    private static class NoopLog implements ContextLog {
        NoopLog() {
        }

        @Override
        public long ended() {
            return 0;
        }
    }

    private static class  NoopLogEstimator extends SizeEstimator<NoopLog> {
        @Override
        public long estimateSize(@Nullable NoopLog value) {
            return 0L;
        }
    }

    @Before
    public void createScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void terminateScheduler() throws InterruptedException {
        if (logSink != null) {
            logSink.close();
        }
        terminate(scheduler);
    }

    public static CircuitBreaker breaker() {
        CircuitBreaker circuitBreaker = mock(CircuitBreaker.class);
        // mocked CircuitBreaker has unlimited memory (⌐■_■)
        when(circuitBreaker.getLimit()).thenReturn(Long.MAX_VALUE);
        return circuitBreaker;
    }

    @Test
    public void testFixedSizeRamAccountingQueueSink() throws Exception {
        BlockingEvictingQueue<NoopLog> q = new BlockingEvictingQueue<>(15_000);
        RamAccountingQueue<NoopLog> ramAccountingQueue = new RamAccountingQueue<>(q, breaker(), NOOP_ESTIMATOR);
        logSink = new QueueSink<>(ramAccountingQueue, ramAccountingQueue::release);

        int THREADS = 50;
        final CountDownLatch latch = new CountDownLatch(THREADS);
        List<Thread> threads = new ArrayList<>(20);
        for (int i = 0; i < THREADS; i++) {
            Thread t = new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    logSink.add(new NoopLog());
                }

                latch.countDown();
            });
            t.start();
            threads.add(t);
        }

        latch.await();
        assertThat(ramAccountingQueue.size(), is(15_000));
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testRemoveExpiredLogs() {
        StatementClassifier.Classification classification =
            new StatementClassifier.Classification(Plan.StatementType.SELECT, Collections.singleton("Collect"));
        ConcurrentLinkedQueue<JobContextLog> q = new ConcurrentLinkedQueue<>();
        ScheduledFuture<?> task = TimeBasedQEviction.scheduleTruncate(
            1_000_000L,
            1_000_000L,
            q,
            scheduler,
            TimeValue.timeValueSeconds(1L)
        );
        q.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff01"),
            "select 1", 1L, User.CRATE_USER, classification), null, 2000L));
        q.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff02"),
            "select 1", 1L, User.CRATE_USER, classification), null, 4000L));
        q.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff03"),
            "select 1", 1L, User.CRATE_USER, classification), null, 7000L));

        TimeBasedQEviction.removeExpiredLogs(q, 10_000L, 5_000L);
        assertThat(q.size(), is(1));
        assertThat(q.iterator().next().id(), is(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff03")));

        task.cancel(true);
    }

    @Test
    public void testTimedRamAccountingQueueSink() throws Exception {
        ConcurrentLinkedQueue<NoopLog> q = new ConcurrentLinkedQueue<>();
        RamAccountingQueue<NoopLog> ramAccountingQueue = new RamAccountingQueue<>(q, breaker(), NOOP_ESTIMATOR);
        TimeValue timeValue = TimeValue.timeValueSeconds(1L);
        ScheduledFuture<?> task = TimeBasedQEviction.scheduleTruncate(1000L, 1000L, q, scheduler, timeValue);
        logSink = new QueueSink<>(ramAccountingQueue, () -> {
            task.cancel(false);
            ramAccountingQueue.release();
        });

        for (int j = 0; j < 100; j++) {
            logSink.add(new NoopLog());
        }
        assertThat(ramAccountingQueue.size(), is(100));
        Thread.sleep(2000L);
        assertThat(ramAccountingQueue.size(), is(0));
    }
}

/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.collect.stats;

import io.crate.breaker.SizeEstimator;
import io.crate.core.collections.BlockingEvictingQueue;
import io.crate.expression.reference.sys.job.ContextLog;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
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

public class RamAccountingQueueSinkTest extends CrateUnitTest {

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
        logSink = new QueueSink<>(ramAccountingQueue, ramAccountingQueue::close);

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
        ConcurrentLinkedQueue<JobContextLog> q = new ConcurrentLinkedQueue<>();
        ScheduledFuture<?> task = new TimeExpiring(1_000_000L, 1_000_000L).registerTruncateTask(q, scheduler, TimeValue.timeValueSeconds(1L));
        q.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff01"),
            "select 1", 1L, null), null, 2000L));
        q.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff02"),
            "select 1", 1L, null), null, 4000L));
        q.add(new JobContextLog(new JobContext(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff03"),
            "select 1", 1L, null), null, 7000L));

        TimeExpiring.instance().removeExpiredLogs(q, 10_000L, 5_000L);
        assertThat(q.size(), is(1));
        assertThat(q.iterator().next().id(), is(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff03")));

        task.cancel(true);
    }

    @Test
    public void testTimedRamAccountingQueueSink() throws Exception {
        ConcurrentLinkedQueue<NoopLog> q = new ConcurrentLinkedQueue<>();
        RamAccountingQueue<NoopLog> ramAccountingQueue = new RamAccountingQueue<>(q, breaker(), NOOP_ESTIMATOR);
        TimeValue timeValue = TimeValue.timeValueSeconds(1L);
        TimeExpiring timeExiring = new TimeExpiring(1000L, 1000L);
        ScheduledFuture<?> task = timeExiring.registerTruncateTask(q, scheduler, timeValue);
        logSink = new QueueSink<>(ramAccountingQueue, () -> {
            task.cancel(false);
            ramAccountingQueue.close();
        });

        for (int j = 0; j < 100; j++) {
            logSink.add(new NoopLog());
        }
        assertThat(ramAccountingQueue.size(), is(100));
        Thread.sleep(2000L);
        assertThat(ramAccountingQueue.size(), is(0));
    }
}

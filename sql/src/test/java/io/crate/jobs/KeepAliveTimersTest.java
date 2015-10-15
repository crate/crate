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

package io.crate.jobs;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.job.TransportKeepAliveAction;
import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.*;

import static org.mockito.Mockito.*;

@Repeat(iterations=100)
public class KeepAliveTimersTest extends CrateUnitTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private static final TimeValue TEST_DELAY = TimeValue.timeValueMillis(100);

    private ThreadPool threadPool;
    private ScheduledExecutorService scheduledExecutorService;
    private TransportKeepAliveAction transportKeepAliveAction;

    @Before
    public void prepare() throws Exception {
        scheduledExecutorService = Executors.newScheduledThreadPool(2);

        // need to mock here, as we cannot set estimated_time_interval via settings due to ES ThreadPool bug
        threadPool = mock(ThreadPool.class);
        when(threadPool.estimatedTimeInMillis()).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                return System.currentTimeMillis();
            }
        });
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class))).thenAnswer(new Answer<ScheduledFuture<?>>() {
            @Override
            public ScheduledFuture<?> answer(InvocationOnMock invocation) throws Throwable {
                Runnable runnable = (Runnable)invocation.getArguments()[0];
                TimeValue interval = (TimeValue)invocation.getArguments()[1];
                return scheduledExecutorService.scheduleWithFixedDelay(runnable, interval.getMillis(), interval.getMillis(), TimeUnit.MILLISECONDS);
            }
        });
        transportKeepAliveAction = mock(TransportKeepAliveAction.class);
    }

    @After
    public void cleanup() throws Exception {
        scheduledExecutorService.shutdownNow();
    }

    @Test
    public void testKeepAliveRunnableWithoutResetIsCalled() throws Exception {
        Tuple<SettableFuture<Void>, KeepAliveTimers.ResettableTimer> futureAndTimer = getTimer(TimeValue.timeValueMillis(10));
        KeepAliveTimers.ResettableTimer timer = futureAndTimer.v2();
        SettableFuture<Void> future = futureAndTimer.v1();
        timer.start();
        future.get(50, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testKeepAliveRunnableWithHighDelayNotCalled() throws Exception {
        Tuple<SettableFuture<Void>, KeepAliveTimers.ResettableTimer> futureAndTimer = getTimer(TimeValue.timeValueMillis(500));
        KeepAliveTimers.ResettableTimer timer = futureAndTimer.v2();
        SettableFuture<Void> future = futureAndTimer.v1();
        timer.start();
        expectedException.expect(TimeoutException.class);
        future.get(100, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testKeepAliveRunnableNotCalledWithManyResets() throws Exception {
        Tuple<SettableFuture<Void>, KeepAliveTimers.ResettableTimer> futureAndTimer = getTimer(TimeValue.timeValueMillis(50));
        final KeepAliveTimers.ResettableTimer timer = futureAndTimer.v2();
        SettableFuture<Void> future = futureAndTimer.v1();

        timer.start();
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                timer.reset();
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
        expectedException.expect(TimeoutException.class);
        future.get(100, TimeUnit.MILLISECONDS);
    }

    private Tuple<SettableFuture<Void>, KeepAliveTimers.ResettableTimer> getTimer(TimeValue timerDelay) {
        KeepAliveTimers keepAliveTimers = new KeepAliveTimers(threadPool, TEST_DELAY, transportKeepAliveAction);
        final SettableFuture<Void> future = SettableFuture.create();
        final KeepAliveTimers.ResettableTimer timer = keepAliveTimers.forRunnable(new Runnable() {
            @Override
            public void run() {
                future.set(null);
            }
        }, timerDelay);
        return new Tuple<>(future, timer);
    }

    @Test
    public void testCanceledBeforeExecution() throws Exception {
        Tuple<SettableFuture<Void>, KeepAliveTimers.ResettableTimer> futureAndTimer = getTimer(TimeValue.timeValueSeconds(1));
        final KeepAliveTimers.ResettableTimer timer = futureAndTimer.v2();
        SettableFuture<Void> future = futureAndTimer.v1();
        timer.start();
        Thread.sleep(10);
        timer.cancel();
        expectedException.expect(TimeoutException.class);
        future.get(100, TimeUnit.MILLISECONDS);

    }
}

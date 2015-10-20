/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.distributed;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.*;
import io.crate.Streamer;
import io.crate.action.job.KeepAliveRequest;
import io.crate.action.job.TransportKeepAliveAction;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.transport.Transports;
import io.crate.jobs.ExecutionState;
import io.crate.jobs.JobContextService;
import io.crate.jobs.KeepAliveTimers;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.RowSender;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class DistributingDownstreamTest extends CrateUnitTest {

    private ListeningExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ThreadPool threadPool;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        // need to mock here, as we cannot set estimated_time_interval via settings due to ES ThreadPool bug
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(executorService);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                return System.currentTimeMillis();
            }
        }).when(threadPool).estimatedTimeInMillis();
        when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class))).thenAnswer(new Answer<ScheduledFuture<?>>() {
            @Override
            public ScheduledFuture<?> answer(InvocationOnMock invocation) throws Throwable {

                Runnable runnable = (Runnable)invocation.getArguments()[0];
                TimeValue interval = (TimeValue)invocation.getArguments()[1];
                return scheduledExecutorService.scheduleWithFixedDelay(runnable, interval.millis(), interval.millis(), TimeUnit.MILLISECONDS);
            }
        });
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
        scheduledExecutorService.shutdownNow();
    }

    @Test
    public void testPauseResume() throws Exception {
        final SettableFuture<Boolean> allRowsReceived = SettableFuture.create();
        final List<Row> receivedRows = Collections.synchronizedList(new ArrayList<Row>());
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
                mock(Transports.class),
                mock(JobContextService.class),
                mock(ThreadPool.class),
                mock(TransportService.class)) {


            @Override
            public void pushResult(String node, final DistributedResultRequest request, final ActionListener<DistributedResultResponse> listener) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(randomIntBetween(10, 50));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        final Bucket rows = request.rows();
                        for (Row row : rows) {
                            receivedRows.add(row);
                        }
                        if (request.isLast()) {
                            allRowsReceived.set(true);
                        }
                        listener.onResponse(new DistributedResultResponse(true));
                    }
                });
            }
        };

        Streamer[] streamers = new Streamer[] {DataTypes.STRING.streamer() };
        int pageSize = 10;
        final DistributingDownstream distributingDownstream = new DistributingDownstream(
                UUID.randomUUID(),
                new BroadcastingBucketBuilder(streamers, 1),
                1,
                (byte) 0,
                0,
                ImmutableList.of("n1"),
                transportDistributedResultAction,
                mock(KeepAliveTimers.class, Answers.RETURNS_MOCKS.get()),
                streamers,
                pageSize
        );

        final List<Row> rows = new ArrayList<>();
        for (int i = 65; i < 91; i++) {
            rows.add(new Row1(new BytesRef(Character.toString((char) i))));
            rows.add(new Row1(new BytesRef(Character.toString((char) (i + 32)))));
        }

        final RowSender task1 = new RowSender(rows, distributingDownstream, executorService);

        final ListenableFuture<?> f1 = executorService.submit(task1);

        Futures.allAsList(f1).get(2, TimeUnit.SECONDS);
        allRowsReceived.get(2, TimeUnit.SECONDS);

        assertThat(receivedRows.size(), is(52));
        assertThat(task1.numPauses(), Matchers.greaterThan(0));
        assertThat(task1.numResumes(), Matchers.greaterThan(0));
    }

    @Test
    public void testDownstreamKeepAlive() throws Exception {
        TransportKeepAliveAction transportKeepAliveAction = mock(TransportKeepAliveAction.class);
        final CountDownLatch countDownLatch = new CountDownLatch(3);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                countDownLatch.countDown();
                ActionListener<TransportResponse.Empty> actionListener = (ActionListener<TransportResponse.Empty>) invocation.getArguments()[2];
                actionListener.onResponse(TransportResponse.Empty.INSTANCE);
                return null;
            }
        }).when(transportKeepAliveAction).keepAlive(anyString(), Mockito.any(KeepAliveRequest.class), Mockito.<ActionListener<TransportResponse.Empty>>any());
        KeepAliveTimers keepAliveTimers = new KeepAliveTimers(threadPool, TimeValue.timeValueMillis(10), transportKeepAliveAction);
        Streamer[] streamers = new Streamer[] {DataTypes.STRING.streamer() };
        int pageSize = 10;
        final DistributingDownstream distributingDownstream = new DistributingDownstream(
                UUID.randomUUID(),
                new BroadcastingBucketBuilder(streamers, 1),
                1,
                (byte) 0,
                0,
                ImmutableList.of("n1"),
                mock(TransportDistributedResultAction.class),
                keepAliveTimers,
                streamers,
                pageSize
        );
        distributingDownstream.prepare(mock(ExecutionState.class));
        countDownLatch.await(1, TimeUnit.SECONDS);
    }

    @Test
    public void testTwoDownstreamsOneFinishedOneNeedsMoreDoesNotGetStuck() throws Exception {
        Streamer[] streamers = new Streamer[] {DataTypes.INTEGER.streamer() };

        final AtomicInteger requestsReceived = new AtomicInteger(0);
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
                mock(Transports.class),
                mock(JobContextService.class),
                mock(ThreadPool.class),
                mock(TransportService.class)) {


            @Override
            public void pushResult(String node, final DistributedResultRequest request, final ActionListener<DistributedResultResponse> listener) {
                if (node.equals("n1")) {
                    listener.onResponse(new DistributedResultResponse(false));
                } else {
                    requestsReceived.incrementAndGet();
                    listener.onResponse(new DistributedResultResponse(!request.isLast()));
                }
            }
        };

        KeepAliveTimers keepAliveTimers = new KeepAliveTimers(threadPool, TimeValue.timeValueMillis(10), mock(TransportKeepAliveAction.class));
        DistributingDownstream dd = new DistributingDownstream(
                UUID.randomUUID(),
                new BroadcastingBucketBuilder(streamers, 2),
                1,
                (byte) 0,
                0,
                ImmutableList.of("n1", "n2"),
                transportDistributedResultAction,
                keepAliveTimers,
                streamers,
                2
        );
        dd.prepare(mock(ExecutionState.class));

        RowSender rowSender = new RowSender(
                Arrays.<Row>asList(
                        new Row1(1),
                        new Row1(2),
                        new Row1(3),
                        new Row1(4),
                        new Row1(5)
                ),
                dd,
                MoreExecutors.directExecutor()
        );
        rowSender.run();
        assertThat(requestsReceived.get(), is(3));
    }
}

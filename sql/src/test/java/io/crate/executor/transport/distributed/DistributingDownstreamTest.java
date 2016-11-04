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
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.jobs.PageDownstreamContext;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.RowGenerator;
import io.crate.testing.RowSender;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class DistributingDownstreamTest extends CrateDummyClusterServiceUnitTest {

    private ListeningExecutorService executorService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testPauseResume() throws Exception {
        final SettableFuture<Boolean> allRowsReceived = SettableFuture.create();
        final List<Row> receivedRows = Collections.synchronizedList(new ArrayList<Row>());
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
            mock(Transports.class),
            mock(JobContextService.class),
            mock(ThreadPool.class),
            MockTransportService.local(Settings.EMPTY, Version.V_5_0_1, THREAD_POOL),
            Settings.EMPTY) {


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

        Streamer[] streamers = new Streamer[]{DataTypes.STRING.streamer()};
        int pageSize = 10;
        final DistributingDownstream distributingDownstream = new DistributingDownstream(
            Loggers.getLogger(PageDownstreamContext.class),
            UUID.randomUUID(),
            new BroadcastingBucketBuilder(streamers, 1),
            1,
            (byte) 0,
            0,
            ImmutableList.of("n1"),
            transportDistributedResultAction,
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
    public void testTwoDownstreamsOneFinishedOneNeedsMoreDoesNotGetStuck() throws Exception {
        Streamer[] streamers = new Streamer[]{DataTypes.INTEGER.streamer()};

        final AtomicInteger requestsReceived = new AtomicInteger(0);
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
            mock(Transports.class),
            mock(JobContextService.class),
            mock(ThreadPool.class),
            MockTransportService.local(Settings.EMPTY, Version.V_5_0_1, THREAD_POOL),
            Settings.EMPTY) {


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

        DistributingDownstream dd = new DistributingDownstream(
            Loggers.getLogger(DistributingDownstream.class),
            UUID.randomUUID(),
            new BroadcastingBucketBuilder(streamers, 2),
            1,
            (byte) 0,
            0,
            ImmutableList.of("n1", "n2"),
            transportDistributedResultAction,
            streamers,
            2
        );
        RowSender rowSender = new RowSender(
            RowGenerator.range(1, 6),
            dd,
            MoreExecutors.directExecutor()
        );
        rowSender.run();
        assertThat(requestsReceived.get(), is(3));
    }

    @Test
    public void testKillFailureIsForwarded() throws Exception {
        Streamer[] streamers = new Streamer[]{DataTypes.INTEGER.streamer()};
        AtomicReference<Throwable> throwableReceived = new AtomicReference<>();
        TransportDistributedResultAction transportDistributedResultAction = new TransportDistributedResultAction(
            mock(Transports.class),
            mock(JobContextService.class),
            mock(ThreadPool.class),
            MockTransportService.local(Settings.EMPTY, Version.V_5_0_1, THREAD_POOL),
            Settings.EMPTY) {

            @Override
            public void pushResult(String node, final DistributedResultRequest request, final ActionListener<DistributedResultResponse> listener) {
                if (request.isKilled() && request.throwable() != null) {
                    throwableReceived.set(request.throwable());
                }
            }
        };

        DistributingDownstream dd = new DistributingDownstream(
            Loggers.getLogger(DistributingDownstream.class),
            UUID.randomUUID(),
            new BroadcastingBucketBuilder(streamers, 2),
            1,
            (byte) 0,
            0,
            ImmutableList.of("n1", "n2"),
            transportDistributedResultAction,
            streamers,
            2
        );
        dd.kill(new IllegalStateException("dummy"));

        assertThat(throwableReceived.get(), instanceOf(IllegalStateException.class));
    }
}

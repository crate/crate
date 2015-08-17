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
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.RowSender;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
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

import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class DistributingDownstreamTest extends CrateUnitTest {

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
                streamers,
                pageSize
        );

        final List<Row> rows = new ArrayList<>();
        for (int i = 65; i < 91; i++) {
            rows.add(new Row1(new BytesRef(Character.toString((char) i))));
            rows.add(new Row1(new BytesRef(Character.toString((char) (i + 32)))));
        }
        final RowSender task1 = new RowSender(rows, distributingDownstream, executorService);
        final RowSender task2 = new RowSender(rows, distributingDownstream, executorService);

        final ListenableFuture<?> f1 = executorService.submit(task1);
        final ListenableFuture<?> f2 = executorService.submit(task2);

        Futures.allAsList(f1, f2).get(2, TimeUnit.SECONDS);
        allRowsReceived.get(2, TimeUnit.SECONDS);

        assertThat(receivedRows.size(), is(104));
        assertThat(task1.numPauses(), Matchers.greaterThan(0));
        assertThat(task1.numResumes(), Matchers.greaterThan(0));
        assertThat(task2.numPauses(), Matchers.greaterThan(0));
        assertThat(task2.numResumes(), Matchers.greaterThan(0));
    }
}

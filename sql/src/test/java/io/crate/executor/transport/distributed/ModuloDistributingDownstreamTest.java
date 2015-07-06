/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import io.crate.Constants;
import io.crate.Streamer;
import io.crate.core.collections.Row1;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class ModuloDistributingDownstreamTest extends CrateUnitTest {

    private TransportDistributedResultAction distributedResultAction;
    private TransportDistributedResultAction failingDistributedResultAction;
    private DistributingDownstream downstream;
    private ThreadPool threadPool;

    @Captor
    public ArgumentCaptor<ActionListener<DistributedResultResponse>> listenerArgumentCaptor;
    private int originalPageSize;

    @Before
    public void before() throws Exception {
        originalPageSize = Constants.PAGE_SIZE;
        threadPool = new ThreadPool("dummy");

        distributedResultAction = spy(new TransportDistributedResultAction(
                mock(Transports.class),
                mock(JobContextService.class),
                mock(ThreadPool.class),
                mock(TransportService.class)) {

            @Override
            public void pushResult(String node, DistributedResultRequest request, final ActionListener<DistributedResultResponse> listener) {
                threadPool.executor("same").execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onResponse(new DistributedResultResponse(true));
                    }
                });
            }
        });

        failingDistributedResultAction = spy(new TransportDistributedResultAction(
                mock(Transports.class),
                mock(JobContextService.class),
                mock(ThreadPool.class),
                mock(TransportService.class)) {

            @Override
            public void pushResult(String node, DistributedResultRequest request, final ActionListener<DistributedResultResponse> listener) {
                threadPool.executor("same").execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onFailure(new IllegalStateException("epic fail"));
                    }
                });
            }
        });

        initDownstream(false);
    }

    @After
    public void after() throws Exception {
        Constants.PAGE_SIZE = originalPageSize;
        threadPool.shutdown();
        threadPool.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    private void initDownstream(boolean useFailingTransport) {
        List<String> downstreamNodes = Arrays.asList("n1", "n2");
        Streamer<?>[] streamers = {DataTypes.STRING.streamer()};
        downstream = new ModuloDistributingDownstream(
                UUID.randomUUID(),
                1,
                0,
                downstreamNodes,
                useFailingTransport ? failingDistributedResultAction : distributedResultAction,
                streamers
        );
        downstream.registerUpstream(null);
    }

    @Test
    public void testBucketing() throws Exception {
        ArgumentCaptor<DistributedResultRequest> r1Captor = ArgumentCaptor.forClass(DistributedResultRequest.class);
        doCallRealMethod().when(distributedResultAction).pushResult(eq("n1"), r1Captor.capture(), any(ActionListener.class));

        ArgumentCaptor<DistributedResultRequest> r2Captor = ArgumentCaptor.forClass(DistributedResultRequest.class);
        doCallRealMethod().when(distributedResultAction).pushResult(eq("n2"), r2Captor.capture(), any(ActionListener.class));


        downstream.setNextRow(new Row1(new BytesRef("Trillian")));
        downstream.setNextRow(new Row1(new BytesRef("Marvin")));
        downstream.setNextRow(new Row1(new BytesRef("Arthur")));
        downstream.setNextRow(new Row1(new BytesRef("Slartibartfast")));

        downstream.finish();

        assertRows(r2Captor, "Trillian\nMarvin\n");
        assertRows(r1Captor, "Arthur\nSlartibartfast\n");
    }

    @Test
    public void testOperationIsStoppedOnFailureResponse() throws Exception {
        Constants.PAGE_SIZE = 2;
        // if we modify the PAGE_SIZE, we must re-instantiate the downstream
        // also we need a failing transport here
        initDownstream(true);

        ArgumentCaptor<DistributedResultRequest> captor = ArgumentCaptor.forClass(DistributedResultRequest.class);
        doNothing().when(distributedResultAction).pushResult(any(String.class), captor.capture(), any(ActionListener.class));

        int iterations = 0;
        int expected = 1;
        while (true) {
            if (!downstream.setNextRow(new Row1(new BytesRef("Trillian")))) {
                break;
            }
            if (!downstream.setNextRow(new Row1(new BytesRef("Arthur")))) {
                break;
            }
            iterations++;
        }
        assertThat(iterations, is(expected));
    }

    @Test
    public void testRequestsAreSentWithoutRows() throws Exception {
        ArgumentCaptor<DistributedResultRequest> captor = ArgumentCaptor.forClass(DistributedResultRequest.class);
        doNothing().when(distributedResultAction).pushResult(any(String.class), captor.capture(), any(ActionListener.class));

        downstream.finish();
        assertThat(captor.getAllValues().size(), is(2));
        for (DistributedResultRequest distributedResultRequest : captor.getAllValues()) {
            assertThat(distributedResultRequest.rows().size(), is(0));
        }

    }

    @Test
    public void testNoRequestsSendWhenCancelled() throws Exception {
        downstream.setNextRow(new Row1(new BytesRef("LateNightSprintFinishingAwesomeness")));
        downstream.fail(new CancellationException());

        verify(distributedResultAction, never()).pushResult(any(String.class), any(DistributedResultRequest.class), any(ActionListener.class));

    }

    private void assertRows(ArgumentCaptor<DistributedResultRequest> r2Captor, String expectedRows) {
        List<DistributedResultRequest> allRequestsForNodeN1 = r2Captor.getAllValues();
        assertThat(allRequestsForNodeN1.size(), is(1));
        DistributedResultRequest n1Request = allRequestsForNodeN1.get(0);
        assertThat(n1Request.isLast(), is(true));
        assertThat(n1Request.rowsCanBeRead(), is(true));

        assertThat(TestingHelpers.printedTable(n1Request.rows()), is(expectedRows));
    }
}
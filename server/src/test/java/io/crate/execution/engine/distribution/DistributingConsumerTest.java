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

package io.crate.execution.engine.distribution;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockMakers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import io.crate.Streamer;
import io.crate.data.CollectionBucket;
import io.crate.data.Row;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.BatchSimulatingIterator;
import io.crate.data.testing.FailingBatchIterator;
import io.crate.data.testing.TestingBatchIterators;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.execution.engine.distribution.merge.PassThroughPagingIterator;
import io.crate.execution.jobs.CumulativePageBucketReceiver;
import io.crate.execution.jobs.DistResultRXTask;
import io.crate.execution.jobs.PageBucketReceiver;
import io.crate.execution.support.NodeRequest;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;

public class DistributingConsumerTest extends ESTestCase {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private ExecutorService executorService;

    @Before
    public void setUpExecutor() throws Exception {
        executorService = Executors.newFixedThreadPool(3);
    }

    @After
    public void tearDownExecutor() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void testSendUsingDistributingConsumerAndReceiveWithDistResultRXTask() throws Exception {
        try {
            Streamer<?>[] streamers = {DataTypes.INTEGER.streamer()};
            TestingRowConsumer collectingConsumer = new TestingRowConsumer();
            DistResultRXTask distResultRXTask = createPageDownstreamContext(streamers, collectingConsumer);
            TransportDistributedResultAction distributedResultAction = createFakeTransport(streamers, distResultRXTask);
            DistributingConsumer distributingConsumer = createDistributingConsumer(streamers, distributedResultAction);

            BatchSimulatingIterator<Row> batchSimulatingIterator =
                new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 5),
                    2,
                    3,
                    executorService);
            distributingConsumer.accept(batchSimulatingIterator, null);

            List<Object[]> result = collectingConsumer.getResult();
            assertThat(TestingHelpers.printedTable(new CollectionBucket(result)),
                is("0\n" +
                   "1\n" +
                   "2\n" +
                   "3\n" +
                   "4\n"));

            // pageSize=2 and 5 rows causes 3x pushResult
            verify(distributedResultAction, times(3)).doExecute(any(), any());
        } finally {
            executorService.shutdown();
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDistributingConsumerForwardsFailure() throws Exception {
        Streamer<?>[] streamers = { DataTypes.INTEGER.streamer() };
        TestingRowConsumer collectingConsumer = new TestingRowConsumer();
        DistResultRXTask distResultRXTask = createPageDownstreamContext(streamers, collectingConsumer);
        TransportDistributedResultAction distributedResultAction = createFakeTransport(streamers, distResultRXTask);
        DistributingConsumer distributingConsumer = createDistributingConsumer(streamers, distributedResultAction);

        distributingConsumer.accept(null, new CompletionException(new IllegalArgumentException("foobar")));

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("foobar");
        collectingConsumer.getResult();
    }

    @Test
    public void testFailureOnAllLoadedIsForwarded() throws Exception {
        Streamer<?>[] streamers = { DataTypes.INTEGER.streamer() };
        TestingRowConsumer collectingConsumer = new TestingRowConsumer();
        DistResultRXTask distResultRXTask = createPageDownstreamContext(streamers, collectingConsumer);
        TransportDistributedResultAction distributedResultAction = createFakeTransport(streamers, distResultRXTask);
        DistributingConsumer distributingConsumer = createDistributingConsumer(streamers, distributedResultAction);

        distributingConsumer.accept(FailingBatchIterator.failOnAllLoaded(), null);

        expectedException.expect(InterruptedException.class);
        collectingConsumer.getResult();
    }

    @Test
    public void test_exception_on_loadNextBatch_is_forwarded() throws Exception {
        Streamer<?>[] streamers = { DataTypes.INTEGER.streamer() };
        TestingRowConsumer collectingConsumer = new TestingRowConsumer();
        DistResultRXTask distResultRXTask = createPageDownstreamContext(streamers, collectingConsumer);
        TransportDistributedResultAction distributedResultAction = createFakeTransport(streamers, distResultRXTask);
        DistributingConsumer distributingConsumer = createDistributingConsumer(streamers, distributedResultAction);

        BatchSimulatingIterator<Row> batchSimulatingIterator =
            new BatchSimulatingIterator<>(TestingBatchIterators.range(0, 5),
                                          2,
                                          3,
                                          executorService) {

                @Override
                public CompletionStage<?> loadNextBatch() {
                    throw new CircuitBreakingException("data too large");
                }
            };
        distributingConsumer.accept(batchSimulatingIterator, null);

        expectedException.expect(CircuitBreakingException.class);
        collectingConsumer.getResult();
    }

    private DistributingConsumer createDistributingConsumer(Streamer<?>[] streamers, TransportDistributedResultAction distributedResultAction) {
        return new DistributingConsumer(
            executorService,
            UUID.randomUUID(),
            new ModuloBucketBuilder(streamers, 1, 0, RamAccounting.NO_ACCOUNTING),
            1,
            (byte) 0,
            0,
            Collections.singletonList("n1"),
            distributedResultAction::execute,
            2 // pageSize
        );
    }

    private DistResultRXTask createPageDownstreamContext(Streamer<?>[] streamers, TestingRowConsumer collectingConsumer) {
        PageBucketReceiver pageBucketReceiver = new CumulativePageBucketReceiver(
            "n1",
            1,
            executorService,
            streamers,
            collectingConsumer,
            PassThroughPagingIterator.oneShot(),
            1);

        return new DistResultRXTask(
            1,
            "dummy",
            pageBucketReceiver,
            RamAccounting.NO_ACCOUNTING,
            1
        );
    }

    @SuppressWarnings("unchecked")
    private TransportDistributedResultAction createFakeTransport(Streamer<?>[] streamers, DistResultRXTask distResultRXTask) {
        TransportDistributedResultAction distributedResultAction =
            mock(TransportDistributedResultAction.class , withSettings().mockMaker(MockMakers.SUBCLASS));
        doAnswer((InvocationOnMock invocationOnMock) -> {
            Object[] args = invocationOnMock.getArguments();
            DistributedResultRequest resultRequest = ((NodeRequest<DistributedResultRequest>) args[0]).innerRequest();
            ActionListener<DistributedResultResponse> listener = (ActionListener<DistributedResultResponse>) args[1];
            Throwable throwable = resultRequest.throwable();
            PageBucketReceiver bucketReceiver = distResultRXTask.getBucketReceiver(resultRequest.executionPhaseInputId());
            assertThat(bucketReceiver, Matchers.notNullValue());
            if (throwable == null) {
                bucketReceiver.setBucket(
                    resultRequest.bucketIdx(),
                    resultRequest.readRows(streamers),
                    resultRequest.isLast(),
                    needMore -> listener.onResponse(new DistributedResultResponse(needMore)));
            } else {
                bucketReceiver.kill(throwable);
            }
            return null;
        }).when(distributedResultAction).doExecute(any(), any());
        return distributedResultAction;
    }
}

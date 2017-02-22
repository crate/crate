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

package io.crate.executor.transport.distributed;

import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.CollectionBucket;
import io.crate.jobs.PageDownstreamContext;
import io.crate.operation.merge.PassThroughPagingIterator;
import io.crate.testing.CollectingBatchConsumer;
import io.crate.testing.TestingBatchIterators;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class DistributingConsumerTest {

    private ESLogger logger = Loggers.getLogger(DistributingConsumer.class);

    @Test
    public void testSendUsingDistributingConsumerAndReceiveWithPageDownstreamContext() throws Exception {
        Streamer<?>[] streamers = { DataTypes.INTEGER.streamer() };
        CollectingBatchConsumer collectingConsumer = new CollectingBatchConsumer();
        TransportDistributedResultAction distributedResultAction = mock(TransportDistributedResultAction.class);
        PageDownstreamContext pageDownstreamContext = new PageDownstreamContext(
            logger,
            "n1",
            1,
            "dummy",
            collectingConsumer,
            failure -> {
            },
            PassThroughPagingIterator.oneShot(),
            streamers,
            new RamAccountingContext("dummy", new NoopCircuitBreaker("dummy")),
            1
        );
        doAnswer((InvocationOnMock invocationOnMock) -> {
            Object[] args = invocationOnMock.getArguments();
            DistributedResultRequest resultRequest = (DistributedResultRequest) args[1];
            ActionListener<DistributedResultResponse> listener = (ActionListener<DistributedResultResponse>) args[2];
            resultRequest.streamers(streamers);
            pageDownstreamContext.setBucket(0, resultRequest.rows(), resultRequest.isLast(), needMore -> {
                listener.onResponse(new DistributedResultResponse(needMore));
            });
            return null;
        }).when(distributedResultAction).pushResult(anyString(), any(), any());
        DistributingConsumer distributingConsumer = new DistributingConsumer(
            logger,
            UUID.randomUUID(),
            new ModuloBucketBuilder(streamers, 1, 0),
            1,
            (byte) 0,
            0,
            Collections.singletonList("n1"),
            distributedResultAction,
            streamers,
            2, // pageSize
            new CompletableFuture<>()
        );

        distributingConsumer.accept(TestingBatchIterators.range(0, 5), null);

        List<Object[]> result = collectingConsumer.getResult();
        assertThat(TestingHelpers.printedTable(new CollectionBucket(result)),
            is("0\n" +
               "1\n" +
               "2\n" +
               "3\n" +
               "4\n"));

        // pageSize=2 and 5 rows causes 3x pushResult
        verify(distributedResultAction, times(3)).pushResult(anyString(), any(), any());
    }
}

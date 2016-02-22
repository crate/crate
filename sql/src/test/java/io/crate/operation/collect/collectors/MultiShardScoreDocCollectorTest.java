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

package io.crate.operation.collect.collectors;

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.jobs.KeepAliveListener;
import io.crate.operation.merge.KeyIterable;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.TestingHelpers;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiShardScoreDocCollectorTest {


    @Test
    public void testSingleCollectorGetsExhausted() throws Exception {
        ListeningExecutorService executor = MoreExecutors.newDirectExecutorService();
        Ordering<Row> rowOrdering =
                OrderingByPosition.rowOrdering(new int[]{0}, new boolean[]{false}, new Boolean[]{null});

        List<OrderedDocCollector> collectors = new ArrayList<>();
        collectors.add(mockedCollector(new ShardId("p1", 0), 0, Arrays.<Row>asList(new Row1(1), new Row1(1))));
        collectors.add(mockedCollector(new ShardId("p2", 0), 2, Arrays.<Row>asList(new Row1(2), new Row1(2), new Row1(2))));
        collectors.add(mockedCollector(new ShardId("p1", 1), 10, Arrays.<Row>asList(new Row1(3), new Row1(3))));

        CollectingRowReceiver rowReceiver = CollectingRowReceiver.withLimit(6);
        FlatProjectorChain projectorChain = FlatProjectorChain.withReceivers(Collections.singletonList(rowReceiver));

        MultiShardScoreDocCollector docCollector = new MultiShardScoreDocCollector(
                collectors,
                mock(KeepAliveListener.class),
                rowOrdering,
                projectorChain,
                executor
        );
        docCollector.doCollect();
        Bucket result = rowReceiver.result();

        assertThat(TestingHelpers.printedTable(result), is("1\n1\n2\n2\n2\n2\n"));
    }

    private OrderedDocCollector mockedCollector(final ShardId shardId, final int numRepeats, final Iterable<Row> iterable) throws Exception {
        final OrderedDocCollector collector = mock(OrderedDocCollector.class);
        when(collector.shardId()).thenReturn(shardId);
        when(collector.call()).then(new Answer<Object>() {
            int repeat = 0;

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                if (collector.exhausted) {
                    return new KeyIterable<>(shardId, Collections.emptyList());
                }
                if (repeat++ == numRepeats) {
                    collector.exhausted = true;
                }
                return new KeyIterable<>(shardId, iterable);
            }
        });
        return collector;
    }
}
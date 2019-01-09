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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.common.collections.TreeMapBuilder;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ExecutionPhasesRootTaskTest {

    @Test
    public void testGroupByServer() throws Exception {
        Routing twoNodeRouting = new Routing(TreeMapBuilder.<String, Map<String, IntIndexedContainer>>newMapBuilder()
            .put("node1", TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().put("t1", IntArrayList.from(1, 2)).map())
            .put("node2", TreeMapBuilder.<String, IntIndexedContainer>newMapBuilder().put("t1", IntArrayList.from(3, 4)).map())
            .map());

        UUID jobId = UUID.randomUUID();
        RoutedCollectPhase c1 = new RoutedCollectPhase(
            jobId,
            1,
            "c1",
            twoNodeRouting,
            RowGranularity.DOC,
            ImmutableList.of(),
            ImmutableList.of(),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );

        MergePhase m1 = new MergePhase(
            jobId,
            2,
            "merge1",
            2,
            1,
            Sets.newHashSet("node3", "node4"),
            ImmutableList.of(),
            ImmutableList.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );

        MergePhase m2 = new MergePhase(
            jobId,
            3,
            "merge2",
            2,
            1,
            Sets.newHashSet("node1", "node3"),
            ImmutableList.of(),
            ImmutableList.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );

        NodeOperation n1 = NodeOperation.withDownstream(c1, m1, (byte) 0);
        NodeOperation n2 = NodeOperation.withDownstream(m1, m2, (byte) 0);
        NodeOperation n3 = NodeOperation.withDownstream(m2, mock(ExecutionPhase.class), (byte) 0);

        Map<String, Collection<NodeOperation>> groupByServer = NodeOperationGrouper.groupByServer(ImmutableList.of(n1, n2, n3));

        assertThat(groupByServer.containsKey("node1"), is(true));
        assertThat(groupByServer.get("node1"), Matchers.containsInAnyOrder(n1, n3));

        assertThat(groupByServer.containsKey("node2"), is(true));
        assertThat(groupByServer.get("node2"), Matchers.containsInAnyOrder(n1));

        assertThat(groupByServer.containsKey("node3"), is(true));
        assertThat(groupByServer.get("node3"), Matchers.containsInAnyOrder(n2, n3));

        assertThat(groupByServer.containsKey("node4"), is(true));
        assertThat(groupByServer.get("node4"), Matchers.containsInAnyOrder(n2));
    }
}

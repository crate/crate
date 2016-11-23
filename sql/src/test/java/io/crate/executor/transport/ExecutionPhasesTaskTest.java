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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.operation.NodeOperation;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.node.NodeOperationGrouper;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class ExecutionPhasesTaskTest {

    @Test
    public void testGroupByServer() throws Exception {
        Routing twoNodeRouting = new Routing(TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
            .put("node1", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(1, 2)).map())
            .put("node2", TreeMapBuilder.<String, List<Integer>>newMapBuilder().put("t1", Arrays.asList(3, 4)).map())
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
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_BROADCAST,
            (byte) 0);

        MergePhase m1 = new MergePhase(
            jobId,
            2,
            "merge1",
            2,
            Collections.emptyList(),
            ImmutableList.of(),
            ImmutableList.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
        m1.executionNodes(Sets.newHashSet("node3", "node4"));

        MergePhase m2 = new MergePhase(
            jobId,
            3,
            "merge2",
            2,
            Collections.emptyList(),
            ImmutableList.of(),
            ImmutableList.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
        m2.executionNodes(Sets.newHashSet("node1", "node3"));


        String localNodeId = "node1";
        NodeOperation n1 = NodeOperation.withDownstream(c1, m1, (byte) 0, localNodeId);
        NodeOperation n2 = NodeOperation.withDownstream(m1, m2, (byte) 0, localNodeId);
        NodeOperation n3 = NodeOperation.withDownstream(m2, mock(ExecutionPhase.class), (byte) 0, localNodeId);

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

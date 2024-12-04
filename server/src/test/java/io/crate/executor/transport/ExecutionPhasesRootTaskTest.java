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

package io.crate.executor.transport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.analyze.WhereClause;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.NodeOperationGrouper;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;

public class ExecutionPhasesRootTaskTest {

    @Test
    public void testGroupByServer() throws Exception {
        var routingMap = new TreeMap<String, Map<String, IntIndexedContainer>>();
        routingMap.put("node1", Map.of("t1", IntArrayList.from(1, 2)));
        routingMap.put("node2", Map.of("t1", IntArrayList.from(3, 4)));
        Routing twoNodeRouting = new Routing(routingMap);

        UUID jobId = UUID.randomUUID();
        RoutedCollectPhase c1 = new RoutedCollectPhase(
            jobId,
            1,
            "c1",
            twoNodeRouting,
            RowGranularity.DOC,
            List.of(),
            List.of(),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_BROADCAST
        );

        MergePhase m1 = new MergePhase(
            jobId,
            2,
            "merge1",
            2,
            1,
            Set.of("node3", "node4"),
            List.of(),
            List.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );

        MergePhase m2 = new MergePhase(
            jobId,
            3,
            "merge2",
            2,
            1,
            Set.of("node1", "node3"),
            List.of(),
            List.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );

        NodeOperation n1 = NodeOperation.withDownstream(c1, m1, (byte) 0);
        NodeOperation n2 = NodeOperation.withDownstream(m1, m2, (byte) 0);
        NodeOperation n3 = NodeOperation.withDownstream(m2, mock(ExecutionPhase.class), (byte) 0);

        Map<String, Collection<NodeOperation>> groupByServer = NodeOperationGrouper.groupByServer(List.of(n1, n2, n3));

        assertThat(groupByServer.containsKey("node1")).isTrue();
        assertThat(groupByServer.get("node1")).containsExactlyInAnyOrder(n1, n3);

        assertThat(groupByServer.containsKey("node2")).isTrue();
        assertThat(groupByServer.get("node2")).containsExactly(n1);

        assertThat(groupByServer.containsKey("node3")).isTrue();
        assertThat(groupByServer.get("node3")).containsExactlyInAnyOrder(n2, n3);

        assertThat(groupByServer.containsKey("node4")).isTrue();
        assertThat(groupByServer.get("node4")).containsExactly(n2);
    }
}

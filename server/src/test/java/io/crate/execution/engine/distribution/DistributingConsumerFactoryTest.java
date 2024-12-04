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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.elasticsearch.node.Node;
import org.junit.Before;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.analyze.WhereClause;
import io.crate.data.Paging;
import io.crate.data.RowConsumer;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.types.LongType;

public class DistributingConsumerFactoryTest extends CrateDummyClusterServiceUnitTest {

    private DistributingConsumerFactory rowDownstreamFactory;

    @Before
    public void prepare() {
        rowDownstreamFactory = new DistributingConsumerFactory(
            clusterService,
            THREAD_POOL,
            mock(Node.class)
        );
    }

    private RowConsumer createDownstream(Set<String> downstreamExecutionNodes) {
        UUID jobId = UUID.randomUUID();
        Routing routing = new Routing(
            Map.of("n1", Map.of("i1", IntArrayList.from(1, 2)))
        );
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            jobId,
            1,
            "collect",
            routing,
            RowGranularity.DOC,
            List.of(),
            List.of(),
            WhereClause.MATCH_ALL.queryOrFallback(),
            DistributionInfo.DEFAULT_MODULO
        );
        MergePhase mergePhase = new MergePhase(
            jobId,
            2,
            "merge",
            1,
            1,
            downstreamExecutionNodes,
            List.of(LongType.INSTANCE),
            List.of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
        NodeOperation nodeOperation = NodeOperation.withDownstream(collectPhase, mergePhase, (byte) 0);
        return rowDownstreamFactory.create(nodeOperation, RamAccounting.NO_ACCOUNTING, collectPhase.distributionInfo(), jobId, Paging.PAGE_SIZE);
    }

    @Test
    public void testCreateDownstreamOneNode() throws Exception {
        RowConsumer downstream = createDownstream(Set.of("downstream_node"));
        assertThat(downstream).isExactlyInstanceOf(DistributingConsumer.class);
        assertThat(((DistributingConsumer) downstream).multiBucketBuilder).isExactlyInstanceOf(BroadcastingBucketBuilder.class);
    }

    @Test
    public void testCreateDownstreamMultipleNode() throws Exception {
        RowConsumer downstream = createDownstream(Set.of("downstream_node1", "downstream_node2"));
        assertThat(((DistributingConsumer) downstream).multiBucketBuilder).isExactlyInstanceOf(ModuloBucketBuilder.class);
    }
}

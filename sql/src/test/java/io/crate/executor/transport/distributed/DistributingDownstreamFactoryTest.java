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
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.operation.NodeOperation;
import io.crate.operation.Paging;
import io.crate.operation.projectors.DistributingDownstreamFactory;
import io.crate.operation.projectors.RowReceiver;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.LongType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class DistributingDownstreamFactoryTest extends CrateUnitTest {

    private DistributingDownstreamFactory rowDownstreamFactory;

    @Before
    public void before() {
        rowDownstreamFactory = new DistributingDownstreamFactory(
            Settings.EMPTY,
            new NoopClusterService(),
            mock(TransportDistributedResultAction.class)
        );
    }

    private RowReceiver createDownstream(Set<String> downstreamExecutionNodes) {
        UUID jobId = UUID.randomUUID();
        Routing routing = new Routing(
            TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
                .put("n1", TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                    .put("i1", Arrays.asList(1, 2)).map()).map());
        RoutedCollectPhase collectPhase = new RoutedCollectPhase(
            jobId,
            1,
            "collect",
            routing,
            RowGranularity.DOC,
            ImmutableList.<Symbol>of(),
            ImmutableList.<Projection>of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_MODULO,
            (byte) 0);
        MergePhase mergePhase = new MergePhase(
            jobId,
            2,
            "merge",
            1,
            Collections.emptyList(),
            ImmutableList.<DataType>of(LongType.INSTANCE),
            ImmutableList.<Projection>of(),
            DistributionInfo.DEFAULT_BROADCAST,
            null
        );
        mergePhase.executionNodes(downstreamExecutionNodes);
        NodeOperation nodeOperation = NodeOperation.withDownstream(collectPhase, mergePhase, (byte) 0, "nodeName");
        return rowDownstreamFactory.create(nodeOperation, collectPhase.distributionInfo(), jobId, Paging.PAGE_SIZE);
    }

    @Test
    public void testCreateDownstreamOneNode() throws Exception {
        RowReceiver downstream = createDownstream(ImmutableSet.of("downstream_node"));
        assertThat(downstream, instanceOf(DistributingDownstream.class));
        assertThat(((DistributingDownstream) downstream).multiBucketBuilder, instanceOf(BroadcastingBucketBuilder.class));
    }

    @Test
    public void testCreateDownstreamMultipleNode() throws Exception {
        RowReceiver downstream = createDownstream(ImmutableSet.of("downstream_node1", "downstream_node2"));
        assertThat(((DistributingDownstream) downstream).multiBucketBuilder, instanceOf(ModuloBucketBuilder.class));
    }
}

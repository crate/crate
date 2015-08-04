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

package io.crate.operation.projectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.executor.transport.distributed.BroadcastDistributingDownstream;
import io.crate.executor.transport.distributed.ModuloDistributingDownstream;
import io.crate.executor.transport.distributed.SingleBucketBuilder;
import io.crate.executor.transport.distributed.TransportDistributedResultAction;
import io.crate.metadata.Routing;
import io.crate.operation.NodeOperation;
import io.crate.operation.Paging;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.LongType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.cluster.NoopClusterService;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class InternalResultProviderFactoryTest extends CrateUnitTest {

    private InternalResultProviderFactory resultProviderFactory;

    @Before
    public void before() {
        resultProviderFactory = new InternalResultProviderFactory(
                new NoopClusterService(),
                mock(TransportDistributedResultAction.class),
                ImmutableSettings.EMPTY);
    }

    private ResultProvider createDownstream(Set<String> downstreamExecutionNodes) {
        UUID jobId = UUID.randomUUID();
        Routing routing = new Routing(
                TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
                        .put("n1", TreeMapBuilder.<String, List<Integer>>newMapBuilder()
                                .put("i1", Arrays.asList(1, 2)).map()).map());
        CollectPhase collectPhase = new CollectPhase(jobId, 1, "collect", routing, ImmutableList.<Symbol>of(), ImmutableList.<Projection>of());
        MergePhase mergePhase = new MergePhase(jobId, 2, "merge", 1, ImmutableList.<DataType>of(LongType.INSTANCE), ImmutableList.<Projection>of());
        mergePhase.executionNodes(downstreamExecutionNodes);
        NodeOperation nodeOperation = NodeOperation.withDownstream(collectPhase, mergePhase);
        return resultProviderFactory.createDownstream(nodeOperation, jobId, Paging.PAGE_SIZE);
    }

    @Test
    public void testCreateDownstreamLocalNode() throws Exception {
        ResultProvider downstream = createDownstream(ImmutableSet.<String>of());
        assertThat(downstream, instanceOf(SingleBucketBuilder.class));
    }

    @Test
    public void testCreateDownstreamOneNode() throws Exception {
        ResultProvider downstream = createDownstream(ImmutableSet.of("downstream_node"));
        assertThat(downstream, instanceOf(BroadcastDistributingDownstream.class));
    }

    @Test
    public void testCreateDownstreamMultipleNode() throws Exception {
        ResultProvider downstream = createDownstream(ImmutableSet.of("downstream_node1","downstream_node2"));
        assertThat(downstream, instanceOf(ModuloDistributingDownstream.class));
    }
}

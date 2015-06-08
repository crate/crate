/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import com.google.common.collect.ImmutableMap;
import io.crate.core.collections.TreeMapBuilder;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.*;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.testing.CollectingProjector;
import io.crate.operation.projectors.ResultProvider;
import io.crate.operation.projectors.ResultProviderFactory;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Answers;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.*;

import static io.crate.testing.TestingHelpers.createReference;
import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapSideDataCollectOperationTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testFileUriCollect() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        when(discoveryNode.id()).thenReturn("dummyNodeId");
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.localNodeId()).thenReturn("dummyNodeId");
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(clusterService.state()).thenReturn(clusterState);
        DiscoveryService discoveryService = mock(DiscoveryService.class);
        when(discoveryService.localNode()).thenReturn(discoveryNode);
        IndicesService indicesService = mock(IndicesService.class);
        Functions functions = new Functions(
                ImmutableMap.<FunctionIdent, FunctionImplementation>of(),
                ImmutableMap.<String, DynamicFunctionResolver>of());
        ReferenceResolver referenceResolver = new ReferenceResolver() {
            @Override
            public ReferenceImplementation getImplementation(ReferenceIdent ident) {
                return null;
            }
        };

        NodeSettingsService nodeSettingsService = mock(NodeSettingsService.class);

        MapSideDataCollectOperation collectOperation = new MapSideDataCollectOperation(
                clusterService,
                ImmutableSettings.EMPTY,
                mock(TransportActionProvider.class, Answers.RETURNS_DEEP_STUBS.get()),
                mock(BulkRetryCoordinatorPool.class),
                functions,
                referenceResolver,
                mock(NodeSysExpression.class),
                indicesService,
                new ThreadPool(ImmutableSettings.builder().put("name", getClass().getName()).build(), null),
                new CollectServiceResolver(discoveryService,
                        new SystemCollectService(
                                discoveryService,
                                functions,
                                new StatsTables(ImmutableSettings.EMPTY, nodeSettingsService)
                        )
                ),
                new ResultProviderFactory() {
                    @Override
                    public ResultProvider createDownstream(ExecutionNode node, UUID jobId) {
                        return new CollectingProjector();
                    }
                },
                mock(InformationSchemaCollectService.class),
                mock(UnassignedShardsCollectService.class)
        );

        File tmpFile = temporaryFolder.newFile("fileUriCollectOperation.json");
        try (FileWriter writer = new FileWriter(tmpFile)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }

        Routing routing = new Routing(
                TreeMapBuilder.<String, Map<String, List<Integer>>>newMapBuilder()
                .put("dummyNodeId", new TreeMap<String, List<Integer>>())
                .map()
        );
        FileUriCollectNode collectNode = new FileUriCollectNode(
                0,
                "test",
                routing,
                Literal.newLiteral(Paths.get(tmpFile.toURI()).toUri().toString()),
                Arrays.<Symbol>asList(
                        createReference("name", DataTypes.STRING),
                        createReference(new ColumnIdent("details", "age"), DataTypes.INTEGER)
                ),
                Arrays.<Projection>asList(),
                null,
                false
        );
        collectNode.jobId(UUID.randomUUID());
        PlanNodeBuilder.setOutputTypes(collectNode);
        CollectingProjector cd = new CollectingProjector();
        cd.startProjection(mock(ExecutionState.class));
        collectOperation.collect(collectNode, cd, mock(JobCollectContext.class));
        assertThat(cd.result().get(), contains(
                isRow("Arthur", 38),
                isRow("Trillian", 33)
        ));
    }
}

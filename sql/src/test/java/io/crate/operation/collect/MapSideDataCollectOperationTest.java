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
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.metadata.*;
import io.crate.planner.node.dql.FileUriCollectNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.crate.testing.TestingHelpers.createReference;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapSideDataCollectOperationTest {


    @Test
    public void testFileUriCollect() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        when(discoveryNode.id()).thenReturn("dummyNodeId");
        when(clusterService.localNode()).thenReturn(discoveryNode);
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
                mock(TransportShardBulkAction.class),
                mock(TransportCreateIndexAction.class),
                functions,
                referenceResolver,
                indicesService,
                new ThreadPool(ImmutableSettings.EMPTY, null),
                new CollectServiceResolver(discoveryService,
                    new SystemCollectService(
                            discoveryService,
                            functions,
                            new StatsTables(ImmutableSettings.EMPTY, nodeSettingsService)
                    )
                )
        );

        File tmpFile = File.createTempFile("fileUriCollectOperation", ".json");
        try (FileWriter writer = new FileWriter(tmpFile)) {
            writer.write("{\"name\": \"Arthur\", \"id\": 4, \"details\": {\"age\": 38}}\n");
            writer.write("{\"id\": 5, \"name\": \"Trillian\", \"details\": {\"age\": 33}}\n");
        }

        Routing routing = new Routing(
                ImmutableMap.<String, Map<String, Set<Integer>>>of("dummyNodeId", new HashMap<String, Set<Integer>>())
        );
        FileUriCollectNode collectNode = new FileUriCollectNode(
                "test",
                routing,
                Literal.newLiteral(tmpFile.getAbsolutePath()),
                Arrays.<Symbol>asList(
                        createReference("name", DataTypes.STRING),
                        createReference(new ColumnIdent("details", "age"), DataTypes.INTEGER)
                ),
                Arrays.<Projection>asList(),
                null,
                false
        );
        ListenableFuture<Object[][]> resultFuture = collectOperation.collect(collectNode);
        Object[][] objects = resultFuture.get();

        assertThat((String)objects[0][0], is("Arthur"));
        assertThat((Integer)objects[0][1], is(38));

        assertThat((String)objects[1][0], is("Trillian"));
        assertThat((Integer)objects[1][1], is(33));

    }
}

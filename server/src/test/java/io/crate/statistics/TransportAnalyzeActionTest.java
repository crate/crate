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

package io.crate.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.crate.fdw.ForeignDataWrapper;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.fdw.ForeignTableStats;
import io.crate.fdw.ServersMetadata;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;


public class TransportAnalyzeActionTest extends ESTestCase {

    @Test
    public void test_create_stats_for_tables_with_array_columns_with_nulls() {

        ArrayType<String> type = DataTypes.STRING_ARRAY;
        var col1 = type.columnStatsSupport().sketchBuilder();
        var col2 = type.columnStatsSupport().sketchBuilder();
        col1.add(null);
        col2.add(null);
        var samples = new Samples(
            List.of(col1, col2),
            2,
            10
        );
        var references = List.<Reference>of(
            new SimpleReference(
                new RelationName(DocSchemaInfo.NAME, "dummy"),
                ColumnIdent.of("dummy"),
                RowGranularity.DOC,
                DataTypes.STRING_ARRAY,
                0,
                null)
        );
        var stats = samples.createTableStats(references);
        assertThat(stats.numDocs()).isEqualTo(2L);
    }

    @Test
    public void test_fetch_samples_generates_and_publishes_foreign_table_stats() throws Exception {
        RelationName relationName = new RelationName("doc", "foreign_tbl");

        RelationMetadata.ForeignTable foreignTable = mock(RelationMetadata.ForeignTable.class);
        when(foreignTable.server()).thenReturn("pg_server");
        when(foreignTable.ident()).thenReturn(relationName);
        when(foreignTable.name()).thenReturn(relationName);

        Settings dummySettings = Settings.builder()
            .put("url", "jdbc:postgresql://dummy")
            .build();
        when(foreignTable.settings()).thenReturn(dummySettings);

        ServersMetadata.Server server = new ServersMetadata.Server("pg_server", "jdbc", "crate", Map.of(), Settings.EMPTY);
        ServersMetadata serversMetadata = mock(ServersMetadata.class);
        when(serversMetadata.get("pg_server")).thenReturn(server);

        Metadata metadata = Metadata.builder(Metadata.OID_UNASSIGNED)
            .putCustom(ServersMetadata.TYPE, serversMetadata)
            .build();

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.metadata()).thenReturn(metadata);

        DiscoveryNode node = mock(DiscoveryNode.class);
        DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.iterator()).thenAnswer(_ -> List.of(node).iterator());
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        SchemaInfo schemaInfo = mock(SchemaInfo.class);
        when(schemaInfo.name()).thenReturn("doc");
        when(schemaInfo.getTables()).thenReturn(List.of(foreignTable));

        Schemas schemas = mock(Schemas.class);
        when(schemas.iterator()).thenAnswer(_ -> List.of(schemaInfo).iterator());

        NodeContext nodeContext = mock(NodeContext.class);
        when(nodeContext.schemas()).thenReturn(schemas);

        ForeignDataWrapper fdw = mock(ForeignDataWrapper.class);
        when(fdw.getStats(any(), any(), any(), any(), any()))
            .thenReturn(CompletableFuture.completedFuture(new ForeignTableStats(100L, 4096L)));

        ForeignDataWrappers foreignDataWrappers = mock(ForeignDataWrappers.class);
        when(foreignDataWrappers.get("jdbc")).thenReturn(fdw);

        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(any())).thenReturn(Runnable::run);

        TransportService transportService = mock(TransportService.class);
        TableStatsService tableStatsService = mock(TableStatsService.class);

        TransportAnalyzeAction analyzeAction = new TransportAnalyzeAction(
            transportService,
            mock(ReservoirSampler.class),
            nodeContext,
            clusterService,
            tableStatsService,
            threadPool,
            foreignDataWrappers
        );

        analyzeAction.fetchSamplesThenGenerateAndPublishStats().get();

        ArgumentCaptor<RelationMetadata.ForeignTable> tableCaptor = ArgumentCaptor.forClass(RelationMetadata.ForeignTable.class);
        verify(fdw).getStats(any(), any(), tableCaptor.capture(), any(), any());
        assertThat(tableCaptor.getValue().ident()).isEqualTo(relationName);

        ArgumentCaptor<PublishTableStatsRequest> requestCaptor = ArgumentCaptor.forClass(PublishTableStatsRequest.class);
        verify(transportService).sendRequest(any(), any(), requestCaptor.capture(), any());

        Map<RelationName, Stats> publishedStats = requestCaptor.getValue().tableStats();
        assertThat(publishedStats).containsKey(relationName);
        assertThat(publishedStats.get(relationName).numDocs()).isEqualTo(100L);
        assertThat(publishedStats.get(relationName).sizeInBytes()).isEqualTo(4096L);
    }
}

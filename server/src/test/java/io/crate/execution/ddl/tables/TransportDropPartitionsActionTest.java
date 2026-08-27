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

package io.crate.execution.ddl.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TransportDropPartitionsActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_uses_table_oid_if_source_name_points_to_another_table() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (p int) partitioned by (p)", List.of("1"))
            .addTable("create table doc.t2 (p int) partitioned by (p)", List.of("1"));
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        var action = new TransportDropPartitionsAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            new MetadataDeleteIndexService(Settings.EMPTY, allocationService)
        );

        ClusterState result = action.clusterStateTaskExecutor(null)
            .execute(
                clusterService.state(),
                List.of(new DropPartitionsRequest(
                    t2Name,
                    metadata.getRelation(t1Name).oid(),
                    List.of(new PartitionName(t1Name, List.of("1")))
                ))
            ).resultingState;

        String originalT1IndexUUID = metadata.getIndex(t1Name, List.of("1"), true, IndexMetadata::getIndexUUID);
        String originalT2IndexUUID = metadata.getIndex(t2Name, List.of("1"), true, IndexMetadata::getIndexUUID);
        assertThat(result.metadata().index(originalT1IndexUUID)).isNull();
        assertThat(result.metadata().index(originalT2IndexUUID)).isNotNull();
    }

    @Test
    public void test_falls_back_to_relation_name_if_table_oid_is_unassigned() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table doc.t1 (p int) partitioned by (p)", List.of("1"))
            .addTable("create table doc.t2 (p int) partitioned by (p)", List.of("1"));
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        var action = new TransportDropPartitionsAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            new MetadataDeleteIndexService(Settings.EMPTY, allocationService)
        );

        ClusterState result = action.clusterStateTaskExecutor(null)
            .execute(
                clusterService.state(),
                List.of(new DropPartitionsRequest(
                    t2Name,
                    OID_UNASSIGNED,
                    List.of(new PartitionName(t1Name, List.of("1")))
                ))
            ).resultingState;

        String originalT1IndexUUID = metadata.getIndex(t1Name, List.of("1"), true, IndexMetadata::getIndexUUID);
        String originalT2IndexUUID = metadata.getIndex(t2Name, List.of("1"), true, IndexMetadata::getIndexUUID);
        assertThat(result.metadata().index(originalT1IndexUUID)).isNotNull();
        assertThat(result.metadata().index(originalT2IndexUUID)).isNull();
    }
}

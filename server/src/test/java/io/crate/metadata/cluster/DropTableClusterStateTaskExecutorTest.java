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

package io.crate.metadata.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.cluster.metadata.Metadata.OID_UNASSIGNED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.execution.ddl.tables.DropTableRequest;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DropTableClusterStateTaskExecutorTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_uses_table_oid_if_source_name_points_to_another_table() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x int)");
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        RelationMetadata.Table t1 = metadata.getRelation(t1Name);

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        var executor = new DropTableClusterStateTaskExecutor(
            new MetadataDeleteIndexService(Settings.EMPTY, allocationService),
            new DDLClusterStateService()
        );
        ClusterState result = executor.execute(clusterService.state(), new DropTableRequest(t2Name, t1.oid()));
        Metadata resultMetadata = result.metadata();

        assertThat((RelationMetadata) resultMetadata.getRelation(t1Name)).isNull();
        assertThat((RelationMetadata) resultMetadata.getRelation(t2Name)).isNotNull();
    }

    @Test
    public void test_falls_back_to_relation_name_if_table_oid_is_unassigned() throws Exception {
        var e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x int)");
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));

        var executor = new DropTableClusterStateTaskExecutor(
            new MetadataDeleteIndexService(Settings.EMPTY, allocationService),
            new DDLClusterStateService()
        );
        ClusterState result = executor.execute(clusterService.state(), new DropTableRequest(t2Name, OID_UNASSIGNED));
        Metadata resultMetadata = result.metadata();

        assertThat((RelationMetadata) resultMetadata.getRelation(t1Name)).isNotNull();
        assertThat((RelationMetadata) resultMetadata.getRelation(t2Name)).isNull();
    }
}

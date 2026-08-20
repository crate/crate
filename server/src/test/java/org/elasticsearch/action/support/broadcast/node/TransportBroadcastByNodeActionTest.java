/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package org.elasticsearch.action.support.broadcast.node;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class TransportBroadcastByNodeActionTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_indices_stats_request_uses_table_oid_if_source_name_points_to_another_table() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable("create table t1 (x int)")
            .addTable("create table t2 (x int)");
        RelationName t1Name = new RelationName("doc", "t1");
        RelationName t2Name = new RelationName("doc", "t2");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        RelationMetadata.Table t1 = metadata.getRelation(t1Name);

        Metadata metadataAfterSwap = swapRelations(metadata, t1Name, t2Name);
        ClusterState stateAfterSwap = ClusterState.builder(clusterService.state())
            .metadata(metadataAfterSwap)
            .build();

        String[] concreteIndices = TransportBroadcastByNodeAction.concreteIndices(
            stateAfterSwap,
            new IndicesStatsRequest(new PartitionName(t1Name, List.of()), t1.oid())
        );

        String t1IndexUUID = metadata.getIndex(t1Name, List.of(), true, IndexMetadata::getIndexUUID);
        assertThat(concreteIndices).containsExactly(t1IndexUUID);
    }

    @Test
    public void test_indices_stats_request_uses_table_oid_and_partition_values() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService)
            .addTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS
            )
            .addTable("create table other (x int)");
        RelationName partedName = new RelationName("doc", "parted");
        RelationName otherName = new RelationName("doc", "other");
        List<String> partitionValues = List.of("1395874800000");

        Metadata metadata = e.getPlannerContext().clusterState().metadata();
        RelationMetadata.Table parted = metadata.getRelation(partedName);

        Metadata metadataAfterSwap = swapRelations(metadata, partedName, otherName);
        ClusterState stateAfterSwap = ClusterState.builder(clusterService.state())
            .metadata(metadataAfterSwap)
            .build();

        String[] concreteIndices = TransportBroadcastByNodeAction.concreteIndices(
            stateAfterSwap,
            new IndicesStatsRequest(new PartitionName(partedName, partitionValues), parted.oid())
        );

        String partedIndexUUID = metadata.getIndex(partedName, partitionValues, true, IndexMetadata::getIndexUUID);
        assertThat(concreteIndices).containsExactly(partedIndexUUID);
    }

    private static Metadata swapRelations(Metadata metadata, RelationName leftName, RelationName rightName) {
        RelationMetadata.Table left = metadata.getRelation(leftName);
        RelationMetadata.Table right = metadata.getRelation(rightName);
        return Metadata.builder(metadata)
            .dropRelation(leftName)
            .dropRelation(rightName)
            .setTable(
                leftName,
                right.columns().stream().map(ref -> ref.withRelation(leftName)).toList(),
                right.settings(),
                right.routingColumn(),
                right.columnPolicy(),
                right.pkConstraintName(),
                right.checkConstraints(),
                right.primaryKeys(),
                right.partitionedBy(),
                right.state(),
                right.indexUUIDs(),
                right.tableVersion() + 1,
                right.oid()
            )
            .setTable(
                rightName,
                left.columns().stream().map(ref -> ref.withRelation(rightName)).toList(),
                left.settings(),
                left.routingColumn(),
                left.columnPolicy(),
                left.pkConstraintName(),
                left.checkConstraints(),
                left.primaryKeys(),
                left.partitionedBy(),
                left.state(),
                left.indexUUIDs(),
                left.tableVersion() + 1,
                left.oid()
            )
            .build();
    }
}

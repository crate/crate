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

package io.crate.expression.reference.sys.snapshot;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public record SysSnapshot(String uuid,
                          String name,
                          String repository,
                          List<RelationName> tables,
                          List<PartitionName> partitions,
                          List<String> concreteIndices,
                          Long started,
                          Long finished,
                          String version,
                          String state,
                          List<String> failures,
                          String reason,
                          int totalShards,
                          Boolean includeGlobalState) {


    @Nullable
    private static IndexMetadata getIndexMetadata(Metadata metadata, String indexName) {
        for (var cursor : metadata.indices().values()) {
            IndexMetadata indexMetadata = cursor.value;
            if (indexMetadata.getIndex().getName().equals(indexName)) {
                return indexMetadata;
            }
        }
        return null;
    }

    public static SysSnapshot of(Metadata metadata, SnapshotsInProgress.Entry inProgressEntry) {
        Snapshot snapshot = inProgressEntry.snapshot();
        SnapshotId snapshotId = snapshot.getSnapshotId();
        ArrayList<PartitionName> partitions = new ArrayList<>();
        ArrayList<String> indexNames = new ArrayList<>();
        // TODO: Consider changing the inProgressRepresentation to include PartitionName or relationName+partitionValues?
        for (var indexId : inProgressEntry.indices()) {
            String indexName = indexId.getName();
            IndexMetadata indexMetadata = getIndexMetadata(metadata, indexName);
            assert indexMetadata != null
                : "There must be indexMetadata for any index in SnapshotsInProgress.Entry";
            RelationMetadata relation = metadata.getRelation(indexMetadata.getIndexUUID());
            assert relation != null
                : "If an index is in a SnapshotsInProgress.Entry the relationMetadata for it must exist";
            if (!indexMetadata.partitionValues().isEmpty()) {
                partitions.add(new PartitionName(relation.name(), indexMetadata.partitionValues()));
            }
            indexNames.add(indexName);
        }
        return new SysSnapshot(
            snapshotId.getUUID(),
            snapshotId.getName(),
            snapshot.getRepository(),
            inProgressEntry.relationNames(),
            partitions,
            indexNames,
            inProgressEntry.startTime(),
            0L,
            Version.CURRENT.toString(),
            SnapshotState.IN_PROGRESS.name(),
            List.of(),
            inProgressEntry.failure(),
            inProgressEntry.shards().size(),
            inProgressEntry.includeGlobalState()
        );
    }

    public static SysSnapshot of(Metadata snapshotMetadata, String repoName, SnapshotInfo info) {
        SnapshotId snapshotId = info.snapshotId();
        Version version = info.version();
        List<String> indexNames = info.indexNames();
        ArrayList<PartitionName> partitions = new ArrayList<>();
        LinkedHashSet<RelationName> relations = new LinkedHashSet<>();
        relations.addAll(Lists.mapLazy(snapshotMetadata.relations(RelationMetadata.Table.class), RelationMetadata::name));
        for (String indexName : indexNames) {
            IndexMetadata indexMetadata = getIndexMetadata(snapshotMetadata, indexName);
            RelationMetadata relation = indexMetadata == null ? null : snapshotMetadata.getRelation(indexMetadata.getIndexUUID());
            // Old snapshots might not have indexMetadata or relation metadata
            if (relation == null) {
                IndexParts indexParts = IndexName.decode(indexName);
                if (indexParts.isPartitioned()) {
                    partitions.add(indexParts.toPartitionName());
                }
                relations.add(indexParts.toRelationName());
            } else {
                relations.add(relation.name());
                if (!indexMetadata.partitionValues().isEmpty()) {
                    partitions.add(new PartitionName(relation.name(), indexMetadata.partitionValues()));
                }
            }
        }
        return new SysSnapshot(
            snapshotId.getUUID(),
            snapshotId.getName(),
            repoName,
            List.copyOf(relations),
            partitions,
            indexNames,
            info.startTime(),
            info.endTime(),
            version == null ? null : version.toString(),
            info.state().name(),
            Lists.map(info.shardFailures(), SnapshotShardFailure::toString),
            info.reason(),
            info.totalShards(),
            info.includeGlobalState()
        );
    }

    public static SysSnapshot ofMissingInfo(String repoName, SnapshotId snapshotId) {
        return new SysSnapshot(
            snapshotId.getUUID(),
            snapshotId.getName(),
            repoName,
            List.of(),
            List.of(),
            List.of(),
            null,
            null,
            null,
            SnapshotState.FAILED.name(),
            List.of(),
            null, // We don't show info retrieval error, "reason" shows only snapshotting operation error.
            0,
            null
        );
    }

    public List<String> tableNames() {
        return Lists.map(tables, RelationName::fqn);
    }
}

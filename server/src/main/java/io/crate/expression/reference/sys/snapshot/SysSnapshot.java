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

import java.util.List;
import java.util.stream.Stream;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;

import io.crate.common.collections.Lists;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public record SysSnapshot(String uuid,
                          String name,
                          String repository,
                          List<String> concreteIndices,
                          List<String> partitionedTables,
                          Long started,
                          Long finished,
                          String version,
                          String state,
                          List<String> failures,
                          String reason,
                          int totalShards,
                          Boolean includeGlobalState) {

    public static SysSnapshot of(SnapshotsInProgress.Entry inProgressEntry) {
        Snapshot snapshot = inProgressEntry.snapshot();
        SnapshotId snapshotId = snapshot.getSnapshotId();
        return new SysSnapshot(
            snapshotId.getUUID(),
            snapshotId.getName(),
            snapshot.getRepository(),
            Lists.map(inProgressEntry.indices(), IndexId::getName),
            Lists.map(inProgressEntry.relationNames(), RelationName::fqn),
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

    public static SysSnapshot of(Metadata metadata, String repoName, SnapshotInfo info) {
        SnapshotId snapshotId = info.snapshotId();
        Version version = info.version();
        return new SysSnapshot(
            snapshotId.getUUID(),
            snapshotId.getName(),
            repoName,
            info.indices(),
            metadata.relations(RelationMetadata.Table.class).stream()
                .filter(x -> !x.partitionedBy().isEmpty())
                .map(x -> x.name().fqn())
                .toList(),
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


    public List<String> tables() {
        return Stream.concat(concreteIndices.stream().map(RelationName::fqnFromIndexName), partitionedTables.stream())
            .distinct()
            .toList();
    }

    public List<RelationName> relationNames() {
        return Stream.concat(
                concreteIndices.stream().map(RelationName::fromIndexName),
                partitionedTables.stream().map(RelationName::fromIndexName)
            )
            .distinct()
            .toList();
    }

    public List<PartitionName> tablePartitions() {
        return concreteIndices.stream()
            .map(IndexName::decode)
            .filter(IndexParts::isPartitioned)
            .map(indexParts -> new PartitionName(
                new RelationName(indexParts.schema(), indexParts.table()),
                indexParts.partitionIdent()))
            .toList();
    }
}

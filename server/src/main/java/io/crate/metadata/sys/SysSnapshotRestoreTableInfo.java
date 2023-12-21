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

package io.crate.metadata.sys;

import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;

import java.util.Collections;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.RestoreInProgress;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.reference.sys.snapshot.SysSnapshotRestoreInProgress;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class SysSnapshotRestoreTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "snapshot_restore");

    static SystemTable<SysSnapshotRestoreInProgress> create() {
        return SystemTable.<SysSnapshotRestoreInProgress>builder(IDENT)
            .add("id", STRING, SysSnapshotRestoreInProgress::id)
            .add("name", STRING, SysSnapshotRestoreInProgress::name)
            .add("repository", STRING, SysSnapshotRestoreInProgress::repository)
            .startObjectArray("shards", SysSnapshotRestoreInProgress::shards)
                .add("table_schema", STRING, s -> s.indexParts().getSchema())
                .add("table_name", STRING, s -> s.indexParts().getTable())
                .add("partition_ident", STRING, s -> s.indexParts().getPartitionIdent())
                .add("shard_id", INTEGER, SysSnapshotRestoreInProgress.ShardRestoreInfo::id)
                .add("state", STRING, s -> s.state().name())
            .endObjectArray()
            .add("state", STRING, SysSnapshotRestoreInProgress::state)
            .setPrimaryKeys(
                new ColumnIdent("id"),
                new ColumnIdent("name"),
                new ColumnIdent("repository"))
            .withRouting((state, routingProvider, sessionSettings) ->
                             routingProvider.forRandomMasterOrDataNode(IDENT, state.nodes()))
            .build();
    }

    public static Iterable<SysSnapshotRestoreInProgress> snapshotsRestoreInProgress(
        @Nullable RestoreInProgress restoreInProgress) {
        if (restoreInProgress != null) {
            return () -> StreamSupport.stream(restoreInProgress.spliterator(), false)
                .map(SysSnapshotRestoreInProgress::of)
                .iterator();
        } else {
            return Collections::emptyIterator;
        }
    }
}

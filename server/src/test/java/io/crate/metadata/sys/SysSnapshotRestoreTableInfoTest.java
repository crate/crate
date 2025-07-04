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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.junit.Test;

import io.crate.expression.reference.sys.snapshot.SysSnapshotRestoreInProgress;
import io.crate.metadata.RelationName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class SysSnapshotRestoreTableInfoTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_convert_restore_in_progress_to_sys_restore_snapshot_info() throws Exception {
        // Create table so it's metadata can be resolved by the index uuid
        RelationName relationName = new RelationName("doc", "t1");
        SQLExecutor.builder(clusterService).build()
            .addTable("create table doc.t1 (id int)");
        String indexUuid = clusterService.state().metadata().getIndex(relationName, List.of(), true, IndexMetadata::getIndexUUID);

        ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> shardsBuilder
            = ImmutableOpenMap.builder();
        shardsBuilder.put(
            new ShardId(relationName.indexNameOrAlias(), indexUuid, 0),
            new RestoreInProgress.ShardRestoreStatus("nodeId", RestoreInProgress.State.STARTED)
        );
        RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            indexUuid,
            new Snapshot(
                "repository",
                new SnapshotId("snapshot", UUID.randomUUID().toString())),
            RestoreInProgress.State.SUCCESS,
            List.of(relationName.indexNameOrAlias()),
            shardsBuilder.build()
        );

        var restoreInProgressIt = SysSnapshotRestoreTableInfo
            .snapshotsRestoreInProgress(new RestoreInProgress.Builder().add(entry).build(), clusterService.state())
            .iterator();

        assertThat(restoreInProgressIt.hasNext()).isTrue();
        assertThat(restoreInProgressIt.next()).isEqualTo(SysSnapshotRestoreInProgress.of(entry, clusterService.state()));
        assertThat(restoreInProgressIt.hasNext()).isFalse();
    }

    @Test
    public void test_convert_null_restore_in_progress_returns_empty_iterator() {
        var restoreInProgressIt = SysSnapshotRestoreTableInfo.snapshotsRestoreInProgress(null, ClusterState.EMPTY_STATE).iterator();
        assertThat(restoreInProgressIt.hasNext()).isFalse();
    }
}

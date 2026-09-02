/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardState;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import io.crate.metadata.RelationName;

public class SnapshotShardsServiceTests extends ESTestCase {

    @Test
    public void test_duplicate_index_name_in_entry_notifies_failed_snapshot_shard() {
        DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "local").build(), random());
        MockTransport mockTransport = new MockTransport();
        DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        TransportService transportService = mockTransport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            x -> localNode,
            null
        );

        AtomicReference<UpdateIndexShardSnapshotStatusRequest> receivedRequest = new AtomicReference<>();
        transportService.registerRequestHandler(
            SnapshotsService.UPDATE_SNAPSHOT_STATUS_ACTION_NAME,
            ThreadPool.Names.SAME,
            UpdateIndexShardSnapshotStatusRequest::new,
            (request, channel) -> {
                receivedRequest.set(request);
                channel.sendResponse(new UpdateIndexShardSnapshotStatusResponse());
            }
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.localNode()).thenReturn(localNode);

        SnapshotShardsService snapshotShardsService = new SnapshotShardsService(
            Settings.EMPTY,
            clusterService,
            mock(RepositoriesService.class),
            transportService,
            mock(IndicesService.class)
        );

        // Before https://github.com/crate/crate/pull/19734 it was possible to end up
        // with duplicate index names in the cluster.
        // A swapped table before the fix can still experience this issue.
        // This test verifies that such old problematic indices
        // are handled properly when creating next snapshot.
        String indexName = "doc..partitioned.tbl1.04132";
        IndexId duplicateA = new IndexId(indexName, "id-a");
        IndexId duplicateB = new IndexId(indexName, "id-b");

        Index index = new Index(indexName, "index-uuid");
        ShardId shardId = new ShardId(index, 0);

        Map<ShardId, ShardSnapshotStatus> shards = Map.of(
            shardId, new ShardSnapshotStatus(localNode.getId(), "generation-0")
        );
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snapshot", "snapshot-uuid"));
        SnapshotsInProgress.Entry entry = SnapshotsInProgress.startedEntry(
            snapshot,
            true,
            false,
            List.of(duplicateA, duplicateB),
            List.of(new RelationName("doc", "tbl1"), new RelationName("doc", "tbl2")),
            0L,
            1L,
            shards,
            Version.CURRENT
        );

        Map<ShardId, IndexShardSnapshotStatus> startedShards = Map.of(
            shardId, IndexShardSnapshotStatus.newInitializing("generation-0")
        );

        snapshotShardsService.startNewShards(entry, startedShards);
        deterministicTaskQueue.runAllTasks();

        UpdateIndexShardSnapshotStatusRequest notification = receivedRequest.get();
        assertThat(notification)
            .as("failed snapshot shard must be reported")
            .isNotNull();
        assertThat(notification.snapshot()).isEqualTo(snapshot);
        assertThat(notification.shardId()).isEqualTo(shardId);
        assertThat(notification.status().state()).isEqualTo(ShardState.FAILED);
    }
}

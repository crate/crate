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
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class MetadataDeleteIndexServiceTests extends ESTestCase {
    private AllocationService allocationService;
    private MetadataDeleteIndexService service;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(ClusterState.class), any(String.class)))
            .thenAnswer(mockInvocation -> mockInvocation.getArguments()[0]);
        service = new MetadataDeleteIndexService(Settings.EMPTY, null, allocationService);
    }

    public void testDeleteMissing() {
        Index index = new Index("missing", "doesn't matter");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.deleteIndices(state, singleton(index)));
        assertThat(index).isEqualTo(e.getIndex());
    }

    public void testDeleteSnapshotting() {
        String index = randomAlphaOfLength(5);
        Snapshot snapshot = new Snapshot("doesn't matter", new SnapshotId("snapshot name", "snapshot uuid"));
        SnapshotsInProgress snaps = SnapshotsInProgress.of(List.of(new SnapshotsInProgress.Entry(snapshot, true, false,
                SnapshotsInProgress.State.INIT, singletonList(new IndexId(index, "doesn't matter")),
                Collections.emptyList(), System.currentTimeMillis(), (long) randomIntBetween(0, 1000), ImmutableOpenMap.of(), null, VersionUtils.randomVersion(random()))));
        ClusterState state = ClusterState.builder(clusterState(index))
                .putCustom(SnapshotsInProgress.TYPE, snaps)
                .build();
        Exception e = expectThrows(SnapshotInProgressException.class,
                () -> service.deleteIndices(state, singleton(state.metadata().getIndices().get(index).getIndex())));
        assertThat(e.getMessage()).isEqualTo("Cannot delete indices that are being snapshotted: [[" + index + "]]. Try again after snapshot finishes "
            + "or cancel the currently running snapshot.");
    }

    public void testDeleteUnassigned() {
        // Create an unassigned index
        String index = randomAlphaOfLength(5);
        ClusterState before = clusterState(index);

        // Mock the built reroute
        when(allocationService.reroute(any(ClusterState.class), any(String.class))).then(i -> i.getArguments()[0]);

        // Remove it
        ClusterState after = service.deleteIndices(before, singleton(before.metadata().getIndices().get(index).getIndex()));

        // It is gone
        assertThat(after.metadata().getIndices().get(index)).isNull();
        assertThat(after.routingTable().index(index)).isNull();
        assertThat(after.blocks().indices().get(index)).isNull();

        // Make sure we actually attempted to reroute
        verify(allocationService).reroute(any(ClusterState.class), any(String.class));
    }

    private ClusterState clusterState(String index) {
        IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        return ClusterState.builder(ClusterName.DEFAULT)
                .metadata(Metadata.builder().put(indexMetadata, false))
                .routingTable(RoutingTable.builder().addAsNew(indexMetadata).build())
                .blocks(ClusterBlocks.builder().addBlocks(indexMetadata))
                .build();
    }
}

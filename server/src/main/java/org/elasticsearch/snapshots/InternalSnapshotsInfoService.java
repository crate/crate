/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.cursors.ObjectCursor;

public class InternalSnapshotsInfoService implements ClusterStateListener, SnapshotsInfoService {

    public static final Setting<Integer> INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING =
        Setting.intSetting("cluster.snapshot.info.max_concurrent_fetches", 5, 1,
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private static final Logger LOGGER = LogManager.getLogger(InternalSnapshotsInfoService.class);

    private static final ActionListener<ClusterState> REROUTE_LISTENER = ActionListener.wrap(
        r -> LOGGER.trace("reroute after snapshot shard size update completed"),
        e -> LOGGER.debug("reroute after snapshot shard size update failed", e)
    );

    private final ThreadPool threadPool;
    private final Supplier<RepositoriesService> repositoriesService;
    private final Supplier<RerouteService> rerouteService;

    /** contains the snapshot shards for which the size is known **/
    private volatile ImmutableOpenMap<SnapshotShard, Long> knownSnapshotShards;

    private volatile boolean isMaster;

    /** contains the snapshot shards for which the size is unknown and must be fetched (or is being fetched) **/
    private final Set<SnapshotShard> unknownSnapshotShards;

    /** a blocking queue used for concurrent fetching **/
    private final Queue<SnapshotShard> queue;

    /** contains the snapshot shards for which the snapshot shard size retrieval failed **/
    private final Set<SnapshotShard> failedSnapshotShards;

    private volatile int maxConcurrentFetches;
    private int activeFetches;

    private final Object mutex;

    public InternalSnapshotsInfoService(
        final Settings settings,
        final ClusterService clusterService,
        final Supplier<RepositoriesService> repositoriesServiceSupplier,
        final Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.repositoriesService = repositoriesServiceSupplier;
        this.rerouteService = rerouteServiceSupplier;
        this.knownSnapshotShards = ImmutableOpenMap.of();
        this.unknownSnapshotShards = new LinkedHashSet<>();
        this.failedSnapshotShards = new LinkedHashSet<>();
        this.queue = new LinkedList<>();
        this.mutex = new Object();
        this.activeFetches = 0;
        this.maxConcurrentFetches = INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING.get(settings);
        final ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(INTERNAL_SNAPSHOT_INFO_MAX_CONCURRENT_FETCHES_SETTING, this::setMaxConcurrentFetches);
        if (DiscoveryNode.isMasterEligibleNode(settings)) {
            clusterService.addListener(this);
        }
    }

    private void setMaxConcurrentFetches(Integer maxConcurrentFetches) {
        this.maxConcurrentFetches = maxConcurrentFetches;
    }

    @Override
    public SnapshotShardSizeInfo snapshotShardSizes() {
        synchronized (mutex) {
            final ImmutableOpenMap.Builder<SnapshotShard, Long> snapshotShardSizes = ImmutableOpenMap.builder(knownSnapshotShards);
            if (failedSnapshotShards.isEmpty() == false) {
                for (SnapshotShard snapshotShard : failedSnapshotShards) {
                    Long previous = snapshotShardSizes.put(snapshotShard, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                    assert previous == null : "snapshot shard size already known for " + snapshotShard;
                }
            }
            return new SnapshotShardSizeInfo(snapshotShardSizes.build());
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            final Set<SnapshotShard> onGoingSnapshotRecoveries = listOfSnapshotShards(event.state());

            int unknownShards = 0;
            synchronized (mutex) {
                isMaster = true;
                for (SnapshotShard snapshotShard : onGoingSnapshotRecoveries) {
                    // check if already populated entry
                    if (knownSnapshotShards.containsKey(snapshotShard) == false && failedSnapshotShards.contains(snapshotShard) == false) {
                        // check if already fetching snapshot info in progress
                        if (unknownSnapshotShards.add(snapshotShard)) {
                            queue.add(snapshotShard);
                            unknownShards += 1;
                        }
                    }
                }
                // Clean up keys from knownSnapshotShardSizes that are no longer needed for recoveries
                cleanUpSnapshotShardSizes(onGoingSnapshotRecoveries);
            }

            final int nbFetchers = Math.min(unknownShards, maxConcurrentFetches);
            for (int i = 0; i < nbFetchers; i++) {
                fetchNextSnapshotShard();
            }

        } else if (event.previousState().nodes().isLocalNodeElectedMaster()) {
            // TODO Maybe just clear out non-ongoing snapshot recoveries is the node is master eligible, so that we don't
            // have to repopulate the data over and over in an unstable master situation?
            synchronized (mutex) {
                // information only needed on current master
                knownSnapshotShards = ImmutableOpenMap.of();
                failedSnapshotShards.clear();
                isMaster = false;
                SnapshotShard snapshotShard;
                while ((snapshotShard = queue.poll()) != null) {
                    final boolean removed = unknownSnapshotShards.remove(snapshotShard);
                    assert removed : "snapshot shard to remove does not exist " + snapshotShard;
                }
                assert invariant();
            }
        } else {
            synchronized (mutex) {
                assert unknownSnapshotShards.isEmpty() || unknownSnapshotShards.size() == activeFetches;
                assert knownSnapshotShards.isEmpty();
                assert failedSnapshotShards.isEmpty();
                assert isMaster == false;
                assert queue.isEmpty();
            }
        }
    }

    private void fetchNextSnapshotShard() {
        synchronized (mutex) {
            if (activeFetches < maxConcurrentFetches) {
                final SnapshotShard snapshotShard = queue.poll();
                if (snapshotShard != null) {
                    activeFetches += 1;
                    threadPool.generic().execute(new FetchingSnapshotShardSizeRunnable(snapshotShard));
                }
            }
            assert invariant();
        }
    }

    private class FetchingSnapshotShardSizeRunnable extends AbstractRunnable {

        private final SnapshotShard snapshotShard;
        private boolean removed;

        FetchingSnapshotShardSizeRunnable(SnapshotShard snapshotShard) {
            super();
            this.snapshotShard = snapshotShard;
            this.removed = false;
        }

        @Override
        protected void doRun() throws Exception {
            final RepositoriesService repositories = repositoriesService.get();
            assert repositories != null;
            final Repository repository = repositories.repository(snapshotShard.snapshot.getRepository());

            LOGGER.debug("fetching snapshot shard size for {}", snapshotShard);

            repository.getShardSnapshotStatus(
                snapshotShard.snapshot().getSnapshotId(),
                snapshotShard.index(),
                snapshotShard.shardId()).thenAccept(shardSnapshotStatus -> {
                    final long snapshotShardSize = shardSnapshotStatus == null ? 0
                            : shardSnapshotStatus.totalSize();
                    LOGGER.debug("snapshot shard size for {}: {} bytes", snapshotShard, snapshotShardSize);

                    boolean updated = false;
                    synchronized (mutex) {
                        removed = unknownSnapshotShards.remove(snapshotShard);
                        assert removed : "snapshot shard to remove does not exist " + snapshotShardSize;
                        if (isMaster) {
                            final ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes =
                                ImmutableOpenMap.builder(knownSnapshotShards);
                            updated = newSnapshotShardSizes.put(snapshotShard, snapshotShardSize) == null;
                            assert updated : "snapshot shard size already exists for " + snapshotShard;
                            knownSnapshotShards = newSnapshotShardSizes.build();
                        }
                        activeFetches -= 1;
                        assert invariant();
                    }
                    if (updated) {
                        rerouteService.get().reroute("snapshot shard size updated", Priority.HIGH, REROUTE_LISTENER);
                    }
                });
        }

        @Override
        public void onFailure(Exception e) {
            LOGGER.warn(() -> new ParameterizedMessage("failed to retrieve shard size for {}", snapshotShard), e);
            boolean failed = false;
            synchronized (mutex) {
                if (isMaster) {
                    failed = failedSnapshotShards.add(snapshotShard);
                    assert failed : "snapshot shard size already failed for " + snapshotShard;
                }
                if (removed == false) {
                    unknownSnapshotShards.remove(snapshotShard);
                }
                activeFetches -= 1;
                assert invariant();
            }
            if (failed) {
                rerouteService.get().reroute("snapshot shard size failed", Priority.HIGH, REROUTE_LISTENER);
            }
        }

        @Override
        public void onAfter() {
            fetchNextSnapshotShard();
        }
    }

    private void cleanUpSnapshotShardSizes(Set<SnapshotShard> requiredSnapshotShards) {
        assert Thread.holdsLock(mutex);
        ImmutableOpenMap.Builder<SnapshotShard, Long> newSnapshotShardSizes = null;
        for (ObjectCursor<SnapshotShard> shard : knownSnapshotShards.keys()) {
            if (requiredSnapshotShards.contains(shard.value) == false) {
                if (newSnapshotShardSizes == null) {
                    newSnapshotShardSizes = ImmutableOpenMap.builder(knownSnapshotShards);
                }
                newSnapshotShardSizes.remove(shard.value);
            }
        }
        if (newSnapshotShardSizes != null) {
            knownSnapshotShards = newSnapshotShardSizes.build();
        }
        failedSnapshotShards.retainAll(requiredSnapshotShards);
    }

    private boolean invariant() {
        assert Thread.holdsLock(mutex);
        assert activeFetches >= 0 : "active fetches should be greater than or equal to zero but got: " + activeFetches;
        assert activeFetches <= maxConcurrentFetches : activeFetches + " <= " + maxConcurrentFetches;
        for (ObjectCursor<SnapshotShard> cursor : knownSnapshotShards.keys()) {
            assert unknownSnapshotShards.contains(cursor.value) == false : "cannot be known and unknown at same time: " + cursor.value;
            assert failedSnapshotShards.contains(cursor.value) == false : "cannot be known and failed at same time: " + cursor.value;
        }
        for (SnapshotShard shard : unknownSnapshotShards) {
            assert knownSnapshotShards.keys().contains(shard) == false : "cannot be unknown and known at same time: " + shard;
            assert failedSnapshotShards.contains(shard) == false : "cannot be unknown and failed at same time: " + shard;
        }
        for (SnapshotShard shard : failedSnapshotShards) {
            assert knownSnapshotShards.keys().contains(shard) == false : "cannot be failed and known at same time: " + shard;
            assert unknownSnapshotShards.contains(shard) == false : "cannot be failed and unknown at same time: " + shard;
        }
        return true;
    }

    // used in tests
    int numberOfUnknownSnapshotShardSizes() {
        synchronized (mutex) {
            return unknownSnapshotShards.size();
        }
    }

    // used in tests
    int numberOfFailedSnapshotShardSizes() {
        synchronized (mutex) {
            return failedSnapshotShards.size();
        }
    }

    // used in tests
    int numberOfKnownSnapshotShardSizes() {
        return knownSnapshotShards.size();
    }

    private static Set<SnapshotShard> listOfSnapshotShards(final ClusterState state) {
        final Set<SnapshotShard> snapshotShards = new HashSet<>();
        for (ShardRouting shardRouting : state.routingTable().shardsWithState(ShardRoutingState.UNASSIGNED)) {
            if (shardRouting.primary() && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                final RecoverySource.SnapshotRecoverySource snapshotRecoverySource =
                    (RecoverySource.SnapshotRecoverySource) shardRouting.recoverySource();
                final SnapshotShard snapshotShard = new SnapshotShard(snapshotRecoverySource.snapshot(),
                    snapshotRecoverySource.index(), shardRouting.shardId());
                snapshotShards.add(snapshotShard);
            }
        }
        return Collections.unmodifiableSet(snapshotShards);
    }

    public static class SnapshotShard {

        private final Snapshot snapshot;
        private final IndexId index;
        private final ShardId shardId;

        public SnapshotShard(Snapshot snapshot, IndexId index, ShardId shardId) {
            this.snapshot = snapshot;
            this.index = index;
            this.shardId = shardId;
        }

        public Snapshot snapshot() {
            return snapshot;
        }

        public IndexId index() {
            return index;
        }

        public ShardId shardId() {
            return shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final SnapshotShard that = (SnapshotShard) o;
            return shardId.equals(that.shardId)
                && snapshot.equals(that.snapshot)
                && index.equals(that.index);
        }

        @Override
        public int hashCode() {
            return Objects.hash(snapshot, index, shardId);
        }

        @Override
        public String toString() {
            return "[" +
                "snapshot=" + snapshot +
                ", index=" + index +
                ", shard=" + shardId +
                ']';
        }
    }
}

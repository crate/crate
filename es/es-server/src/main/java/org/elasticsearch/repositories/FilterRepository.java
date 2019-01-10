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
package org.elasticsearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.io.IOException;
import java.util.List;

public class FilterRepository implements Repository {

    private final Repository in;

    public FilterRepository(Repository in) {
        this.in = in;
    }

    @Override
    public RepositoryMetaData getMetadata() {
        return in.getMetadata();
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        return in.getSnapshotInfo(snapshotId);
    }

    @Override
    public MetaData getSnapshotGlobalMetaData(SnapshotId snapshotId) {
        return in.getSnapshotGlobalMetaData(snapshotId);
    }

    @Override
    public IndexMetaData getSnapshotIndexMetaData(SnapshotId snapshotId, IndexId index) throws IOException {
        return in.getSnapshotIndexMetaData(snapshotId, index);
    }

    @Override
    public RepositoryData getRepositoryData() {
        return in.getRepositoryData();
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
        in.initializeSnapshot(snapshotId, indices, metaData);
    }

    @Override
    public SnapshotInfo finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure, int totalShards,
                                         List<SnapshotShardFailure> shardFailures, long repositoryStateId, boolean includeGlobalState) {
        return in.finalizeSnapshot(snapshotId, indices, startTime, failure, totalShards, shardFailures, repositoryStateId,
            includeGlobalState);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId) {
        in.deleteSnapshot(snapshotId, repositoryStateId);
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return in.getSnapshotThrottleTimeInNanos();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return in.getRestoreThrottleTimeInNanos();
    }

    @Override
    public String startVerification() {
        return in.startVerification();
    }

    @Override
    public void endVerification(String verificationToken) {
        in.endVerification(verificationToken);
    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {
        in.verify(verificationToken, localNode);
    }

    @Override
    public boolean isReadOnly() {
        return in.isReadOnly();
    }

    @Override
    public void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus) {
        in.snapshotShard(shard, store, snapshotId, indexId, snapshotIndexCommit, snapshotStatus);
    }

    @Override
    public void restoreShard(IndexShard shard, SnapshotId snapshotId, Version version, IndexId indexId, ShardId snapshotShardId,
                             RecoveryState recoveryState) {
        in.restoreShard(shard, snapshotId, version, indexId, snapshotShardId, recoveryState);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        return in.getShardSnapshotStatus(snapshotId, version, indexId, shardId);
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return in.lifecycleState();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        in.addLifecycleListener(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        in.removeLifecycleListener(listener);
    }

    @Override
    public void start() {
        in.start();
    }

    @Override
    public void stop() {
        in.stop();
    }

    @Override
    public void close() {
        in.close();
    }
}

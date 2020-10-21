/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.index.shard;

import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.repositories.RepositoryData.EMPTY_REPO_GEN;

public abstract class RestoreOnlyRepository implements Repository {

    private final String indexName;

    public RestoreOnlyRepository(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return null;
    }

    @Override
    public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
        return null;
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
        return null;
    }

    @Override
    public IndexMetadata getSnapshotIndexMetadata(SnapshotId snapshotId, IndexId index) {
        return null;
    }

    @Override
    public RepositoryData getRepositoryData() {
        HashMap<IndexId, Set<SnapshotId>> map = new HashMap<>();
        map.put(new IndexId(indexName, "blah"), Set.of());
        return new RepositoryData(EMPTY_REPO_GEN, Map.of(), Map.of(), map, ShardGenerations.EMPTY);
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId,
                                 ShardGenerations shardGenerations,
                                 long startTime,
                                 String failure,
                                 int totalShards,
                                 List<SnapshotShardFailure> shardFailures,
                                 long repositoryStateId,
                                 boolean includeGlobalState,
                                 Metadata clusterMetadata,
                                 boolean writeShardGens,
                                 ActionListener<SnapshotInfo> listener) {
        listener.onResponse(null);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId,
                               long repositoryStateId,
                               boolean writeShardGens,
                               ActionListener<Void> listener) {

    }

    @Override
    public String startVerification() {
        return null;
    }

    @Override
    public void endVerification(String verificationToken) {

    }

    @Override
    public void verify(String verificationToken, DiscoveryNode localNode) {

    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void snapshotShard(Store store,
                              MapperService mapperService,
                              SnapshotId snapshotId,
                              IndexId indexId,
                              IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus,
                              boolean writeShardGens, ActionListener<String> listener) {

    }

    @Override
    public void restoreShard(Store store,
                             SnapshotId snapshotId,
                             IndexId indexId,
                             ShardId snapshotShardId,
                             RecoveryState recoveryState) {

    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId,
                                                           IndexId indexId, ShardId shardId) {
        return null;
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return null;
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void close() {

    }
}

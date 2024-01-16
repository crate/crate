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

package io.crate.replication.logical;

import static io.crate.replication.logical.LogicalReplicationSettings.REPLICATION_SUBSCRIPTION_NAME;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusters;
import org.jetbrains.annotations.Nullable;

import io.crate.replication.logical.exceptions.NoSubscriptionForIndexException;

public class ShardReplicationService implements Closeable, IndexEventListener {

    private static final Logger LOGGER = LogManager.getLogger(ShardReplicationService.class);

    private final LogicalReplicationService logicalReplicationService;
    private final LogicalReplicationSettings replicationSettings;
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterName clusterName;
    private final Map<ShardId, ShardReplicationChangesTracker> shards = new ConcurrentHashMap<>();
    private final RemoteClusters remoteClusters;

    @Inject
    public ShardReplicationService(LogicalReplicationService logicalReplicationService,
                                   LogicalReplicationSettings replicationSettings,
                                   RemoteClusters remoteClusters,
                                   Client client,
                                   ThreadPool threadPool,
                                   ClusterService clusterService) {
        this.logicalReplicationService = logicalReplicationService;
        this.replicationSettings = replicationSettings;
        this.remoteClusters = remoteClusters;
        this.client = client;
        this.threadPool = threadPool;
        this.clusterName = clusterService.getClusterName();
    }

    @Override
    public void close() throws IOException {
        for (var tracker : shards.values()) {
            tracker.close();
        }
    }

    CompletableFuture<Client> getRemoteClusterClient(Index index, String subscriptionName) {
        var subscription = logicalReplicationService.subscriptions().get(subscriptionName);
        if (subscription == null) {
            return CompletableFuture.failedFuture(new NoSubscriptionForIndexException(index));
        }
        return remoteClusters.connect(subscriptionName, subscription.connectionInfo());
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.routingEntry().primary()) {
            var subscriptionName = REPLICATION_SUBSCRIPTION_NAME.get(indexShard.indexSettings().getSettings());
            if (subscriptionName == null) {
                return;
            }
            var subscription = logicalReplicationService.subscriptions().get(subscriptionName);
            if (subscription != null) {
                var tracker = new ShardReplicationChangesTracker(
                    subscriptionName,
                    indexShard,
                    threadPool,
                    replicationSettings,
                    ShardReplicationService.this,
                    clusterName.value(),
                    client
                );
                shards.put(indexShard.shardId(), tracker);
                tracker.start();
            }
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId,
                                       @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        closeTracker(shardId);
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        closeTracker(shardId);
    }

    @Override
    public void beforeIndexSettingsChangesApplied(IndexShard indexShard, Settings oldSettings, Settings newSettings) {
        if (REPLICATION_SUBSCRIPTION_NAME.get(oldSettings).isEmpty() == false
            && REPLICATION_SUBSCRIPTION_NAME.get(newSettings).isEmpty() == true) {
            var shardId = indexShard.shardId();
            closeTracker(shardId);
        }
    }

    private void closeTracker(ShardId shardId) {
        var tracker = shards.remove(shardId);
        if (tracker != null) {
            try {
                tracker.close();
            } catch (IOException e) {
                LOGGER.error("Error while closing shard changes tracker of shardId=" + shardId, e);
            }
        }
    }
}

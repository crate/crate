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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.replication.logical.exceptions.NoSubscriptionForIndexException;
import io.crate.replication.logical.metadata.Subscription;

public class ShardReplicationService implements Closeable, IndexEventListener {

    private static final Logger LOGGER = Loggers.getLogger(ShardReplicationService.class);

    private final LogicalReplicationService logicalReplicationService;
    private final LogicalReplicationSettings replicationSettings;
    private final Client client;
    private final ThreadPool threadPool;
    private final Map<ShardId, ShardReplicationChangesTracker> shards = new ConcurrentHashMap<>();
    private final Map<String, String> subscribedIndices = ConcurrentCollections.newConcurrentMap();

    @Inject
    public ShardReplicationService(LogicalReplicationService logicalReplicationService,
                                   LogicalReplicationSettings replicationSettings,
                                   Client client,
                                   ThreadPool threadPool) {
        this.logicalReplicationService = logicalReplicationService;
        this.replicationSettings = replicationSettings;
        this.client = client;
        this.threadPool = threadPool;
    }

    @Override
    public void close() throws IOException {
        for (var tracker : shards.values()) {
            tracker.close();
        }
    }

    void getRemoteClusterClient(Index index, Consumer<Client> onSuccess, Consumer<Exception> onFailure) {
        var subscriptionName = subscriptionName(index.getUUID());
        if (subscriptionName != null) {
            var subscription = logicalReplicationService.subscriptions().get(subscriptionName);
            if (subscription != null) {
                logicalReplicationService.updateRemoteConnection(
                    subscriptionName,
                    subscription.connectionInfo().settings(),
                    ignored -> onSuccess.accept(
                        logicalReplicationService.getRemoteClusterClient(threadPool, subscriptionName)
                    ),
                    onFailure
                );
            } else {
                onFailure.accept(new NoSubscriptionForIndexException(index));
            }

        } else {
            onFailure.accept(new NoSubscriptionForIndexException(index));
        }
    }

    @Nullable
    private String subscriptionName(String indexUUID) {
        return subscribedIndices.get(indexUUID);
    }

    @Nullable
    private Subscription subscription(String indexUUID) {
        var subscriptionName = subscribedIndices.get(indexUUID);
        if (subscriptionName != null) {
            return logicalReplicationService.subscriptions().get(subscriptionName);
        }
        return null;
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        var settings = indexService.getIndexSettings().getSettings();
        var subscriptionName = REPLICATION_SUBSCRIPTION_NAME.get(settings);
        if (subscriptionName != null) {
            subscribedIndices.put(indexService.indexUUID(), subscriptionName);
        }
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        var settings = indexService.getIndexSettings().getSettings();
        var subscriptionName = REPLICATION_SUBSCRIPTION_NAME.get(settings);
        if (subscriptionName != null) {
            subscribedIndices.remove(indexService.indexUUID());
        }
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (indexShard.routingEntry().primary()) {
            var subscription = subscription(indexShard.shardId().getIndex().getUUID());
            if (subscription != null) {
                var tracker = new ShardReplicationChangesTracker(
                    indexShard,
                    replicationSettings,
                    threadPool,
                    ShardReplicationService.this,
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
        var tracker = shards.remove(shardId);
        if (tracker != null) {
            try {
                tracker.close();
            } catch (IOException e) {
                LOGGER.error("Error while closing shard changes tracker of shardId=" + shardId, e);
            }
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
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

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

import io.crate.plugin.IndexEventListenerProxy;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ShardReplicationService implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(ShardReplicationService.class);

    private final Settings settings;
    private final LogicalReplicationService logicalReplicationService;
    private final LogicalReplicationSettings replicationSettings;
    private final Client client;
    private final ThreadPool threadPool;
    private final Map<ShardId, ShardReplicationChangesTracker> shards = new ConcurrentHashMap<>();

    @Inject
    public ShardReplicationService(Settings settings,
                                   IndexEventListenerProxy indexEventListenerProxy,
                                   LogicalReplicationService logicalReplicationService,
                                   LogicalReplicationSettings replicationSettings,
                                   Client client,
                                   ThreadPool threadPool) {
        this.settings = settings;
        this.logicalReplicationService = logicalReplicationService;
        this.replicationSettings = replicationSettings;
        this.client = client;
        this.threadPool = threadPool;
        indexEventListenerProxy.addLast(new LifecycleListener());
    }

    @Override
    public void close() throws IOException {
        for (var tracker : shards.values()) {
            tracker.close();
        }
    }

    @Nullable
    private Client getRemoteClusterClient(String indexName) {
        var subscriptionName = logicalReplicationService.subscriptionName(indexName);
        if (subscriptionName != null) {
            return logicalReplicationService.getRemoteClusterClient(threadPool, subscriptionName);
        }
        return null;
    }

    private class LifecycleListener implements IndexEventListener {

        @Override
        public void afterIndexShardStarted(IndexShard indexShard) {
            if (indexShard.routingEntry().primary()) {
                var subscription = logicalReplicationService.subscription(indexShard.shardId().getIndexName());
                if (subscription != null && subscription.isEnabled()) {
                    var tracker = new ShardReplicationChangesTracker(
                        indexShard,
                        replicationSettings,
                        threadPool,
                        ShardReplicationService.this::getRemoteClusterClient,
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
            if (logicalReplicationService.isSubscribedIndex(shardId.getIndexName())) {
                var tracker = shards.remove(shardId);
                if (tracker != null) {
                    LOGGER.debug("[{}] Closing shard changes tracker on shard close", shardId);
                    try {
                        tracker.close();
                    } catch (IOException e) {
                        LOGGER.debug("Error while closing shard changes tracker of shardId=" + shardId, e);
                    }
                }
            }
        }

        @Override
        public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
            if (logicalReplicationService.isSubscribedIndex(shardId.getIndexName())) {
                var tracker = shards.remove(shardId);
                if (tracker != null) {
                    LOGGER.debug("[{}] Closing shard changes tracker on shard deletion", shardId);
                    try {
                        tracker.close();
                    } catch (IOException e) {
                        LOGGER.debug("Error while closing shard changes tracker of shardId=" + shardId, e);
                    }
                }
            }
        }
    }
}

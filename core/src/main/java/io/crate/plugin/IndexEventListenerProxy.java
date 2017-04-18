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

package io.crate.plugin;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;

/**
 * Class that delegates to one or more IndexEventListener which can be set via
 * {@link #addFirst(IndexEventListener)} or {@link #addLast(IndexEventListener)}
 *
 * This is a bit of a hack:
 *
 *  - IndexEventListener can only be registered on the IndexModule during the initialization of a Plugin
 *  - A IndexEventListener in the SQL module depends on the IndexEventListener of the BLOB module.
 *    The ordering is important, blob must trigger before sql
 *
 * This class acts as a proxy that is created during plugin initialization and enables other components to register
 * real IndexEventListener lazy/later.
 */
public class IndexEventListenerProxy implements IndexEventListener {

    private final List<IndexEventListener> listeners = new ArrayList<>();

    public IndexEventListenerProxy() {
    }

    public void addFirst(IndexEventListener indexEventListener) {
        listeners.add(0, indexEventListener);
    }

    public void addLast(IndexEventListener indexEventListener) {
        listeners.add(indexEventListener);
    }


    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        for (IndexEventListener listener : listeners) {
            listener.shardRoutingChanged(indexShard, oldRouting, newRouting);
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            listener.afterIndexShardCreated(indexShard);
        }
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            listener.afterIndexShardStarted(indexShard);
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.beforeIndexShardClosed(shardId, indexShard, indexSettings);
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.afterIndexShardClosed(shardId, indexShard, indexSettings);
        }
    }

    @Override
    public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState, @Nullable String reason) {
        for (IndexEventListener listener : listeners) {
            listener.indexShardStateChanged(indexShard, previousState, currentState, reason);
        }
    }

    @Override
    public void onShardInactive(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            listener.onShardInactive(indexShard);
        }
    }

    @Override
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.beforeIndexCreated(index, indexSettings);
        }
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        for (IndexEventListener listener : listeners) {
            listener.afterIndexCreated(indexService);
        }
    }

    @Override
    public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.beforeIndexShardCreated(shardId, indexSettings);
        }
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.beforeIndexShardDeleted(shardId, indexSettings);
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.afterIndexShardDeleted(shardId, indexSettings);
        }
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            listener.beforeIndexAddedToCluster(index, indexSettings);
        }
    }

    @Override
    public void onStoreClosed(ShardId shardId) {
        for (IndexEventListener listener : listeners) {
            listener.onStoreClosed(shardId);
        }
    }
}

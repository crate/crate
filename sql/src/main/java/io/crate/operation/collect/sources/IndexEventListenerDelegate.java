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

package io.crate.operation.collect.sources;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;

/**
 * Class that delegates to a IndexEventListener which can be set via {@link #setDelegate(IndexEventListener)}.
 *
 * This is a bit of a hack:
 *
 * IndexEventListeners can only be registered on the IndexModule during the initialization of Plugins.
 * At this time not all dependencies for a concrete IndexEventListener may be available. So this class is registered to
 * enable "lazy" registering of the real IndexEventListener.
 */
public class IndexEventListenerDelegate implements IndexEventListener {

    private final static IndexEventListener SENTINEL = new IndexEventListener() {};

    private IndexEventListener delegate = SENTINEL;

    public IndexEventListenerDelegate() {
    }

    public void setDelegate(IndexEventListener indexEventListener) {
        if (this.delegate == SENTINEL) {
            this.delegate = indexEventListener;
        } else {
            throw new IllegalStateException("delegate already set to: " + delegate);
        }
    }

    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        delegate.shardRoutingChanged(indexShard, oldRouting, newRouting);
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        delegate.afterIndexShardCreated(indexShard);
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        delegate.afterIndexShardStarted(indexShard);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        delegate.beforeIndexShardClosed(shardId, indexShard, indexSettings);
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        delegate.afterIndexShardClosed(shardId, indexShard, indexSettings);
    }

    @Override
    public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState, @Nullable String reason) {
        delegate.indexShardStateChanged(indexShard, previousState, currentState, reason);
    }

    @Override
    public void onShardInactive(IndexShard indexShard) {
        delegate.onShardInactive(indexShard);
    }

    @Override
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        delegate.beforeIndexCreated(index, indexSettings);
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        delegate.afterIndexCreated(indexService);
    }

    @Override
    public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
        delegate.beforeIndexShardCreated(shardId, indexSettings);
    }

    @Override
    public void beforeIndexClosed(IndexService indexService) {
        delegate.beforeIndexClosed(indexService);
    }

    @Override
    public void afterIndexClosed(Index index, Settings indexSettings) {
        delegate.afterIndexClosed(index, indexSettings);
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        delegate.beforeIndexShardDeleted(shardId, indexSettings);
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        delegate.afterIndexShardDeleted(shardId, indexSettings);
    }

    @Override
    public void afterIndexDeleted(Index index, Settings indexSettings) {
        delegate.afterIndexDeleted(index, indexSettings);
    }

    @Override
    public void beforeIndexDeleted(IndexService indexService) {
        delegate.beforeIndexDeleted(indexService);
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        delegate.beforeIndexAddedToCluster(index, indexSettings);
    }

    @Override
    public void onStoreClosed(ShardId shardId) {
        delegate.onStoreClosed(shardId);
    }
}

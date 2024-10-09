/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.collect.collectors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;

import io.crate.common.unit.TimeValue;

public final class ShardStateObserver {

    private static final TimeValue MAX_WAIT_TIME_FOR_NEW_STATE = TimeValue.timeValueSeconds(30);
    private static final Logger LOGGER = LogManager.getLogger(ShardStateObserver.class);

    private final ClusterService clusterService;

    public ShardStateObserver(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public CompletableFuture<ShardRouting> waitForActiveShard(ShardId shardId) {
        ClusterState state = clusterService.state();
        try {
            var routingTable = state.routingTable().shardRoutingTable(shardId);
            var primaryShardRouting = routingTable.primaryShard();
            if (primaryShardRouting.started()) {
                return CompletableFuture.completedFuture(primaryShardRouting);
            }
        } catch (IndexNotFoundException e) {
            return CompletableFuture.failedFuture(e);
        } catch (ShardNotFoundException ignored) {
            // Fall-through to use observer and wait for state update
        }
        var stateObserver = new ClusterStateObserver(
            state, clusterService.getClusterApplierService(), MAX_WAIT_TIME_FOR_NEW_STATE, LOGGER);
        var listener = new RetryIsShardActive(shardId);
        stateObserver.waitForNextChange(listener, newState -> shardStartedOrIndexDeleted(newState, shardId));
        return listener.result();
    }

    private static boolean shardStartedOrIndexDeleted(ClusterState state, ShardId shardId) {
        try {
            return state.routingTable().shardRoutingTable(shardId).primaryShard().started();
        } catch (ShardNotFoundException e) {
            return false;
        } catch (IndexNotFoundException e) {
            return true;
        }
    }

    private static class RetryIsShardActive implements ClusterStateObserver.Listener {

        private final ShardId shardId;

        private final CompletableFuture<ShardRouting> result = new CompletableFuture<>();

        RetryIsShardActive(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            try {
                var routingTable = state.routingTable().shardRoutingTable(shardId);
                var primaryShardRouting = routingTable.primaryShard();
                if (primaryShardRouting.started()) {
                    result.complete(primaryShardRouting);
                }
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        }

        @Override
        public void onClusterServiceClose() {
            result.completeExceptionally(new IllegalStateException(
                "ClusterService was closed while waiting for  shard=" + shardId + " to become active"));
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            result.completeExceptionally(
                new TimeoutException("Timeout waiting for shard=" + shardId + " to become active"));
        }

        public CompletableFuture<ShardRouting> result() {
            return result;
        }
    }
}

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

package org.elasticsearch.action.support;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.node.NodeClosedException;

import io.crate.common.unit.TimeValue;

/**
 * This class provides primitives for waiting for a configured number of shards
 * to become active before sending a response on an {@link ActionListener}.
 */
public class ActiveShardsObserver {

    private static final Logger LOGGER = LogManager.getLogger(ActiveShardsObserver.class);

    private final ClusterService clusterService;

    public ActiveShardsObserver(final ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Creates a listener that will wait for active shards if the provided
     * {@link AcknowledgedResponse} is acked before calling the delegate.
     **/
    public <T extends AcknowledgedResponse> ActionListener<T> waitForShards(
            ActionListener<T> delegate,
            TimeValue timeout,
            Runnable onShardsNotAcknowledged,
            Supplier<String[]> getIndexUUIDs) {
        return ActionListener.wrap(
            resp -> {
                if (resp.isAcknowledged()) {
                    waitForActiveShards(
                        getIndexUUIDs.get(),
                        ActiveShardCount.DEFAULT,
                        timeout,
                        shardsAcknowledged -> {
                            if (!shardsAcknowledged) {
                                onShardsNotAcknowledged.run();
                            }
                            delegate.onResponse(resp);
                        },
                        delegate::onFailure
                    );
                } else {
                    delegate.onResponse(resp);
                }
            },
            delegate::onFailure
        );
    }

    public CompletableFuture<Boolean> waitForActiveShards(String[] indexUUIDs, ActiveShardCount shardCount, TimeValue timeout) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        waitForActiveShards(indexUUIDs, shardCount, timeout, result::complete, result::completeExceptionally);
        return result;
    }

    /**
     * Waits on the specified number of active shards to be started before executing the
     *
     * @param indexUUIDs the indices to wait for active shards on
     * @param activeShardCount the number of active shards to wait on before returning
     * @param timeout the timeout value
     * @param onResult a function that is executed in response to the requisite shards becoming active or a timeout (whichever comes first)
     * @param onFailure a function that is executed in response to an error occurring during waiting for the active shards
     */
    public void waitForActiveShards(final String[] indexUUIDs,
                                    final ActiveShardCount activeShardCount,
                                    final TimeValue timeout,
                                    final Consumer<Boolean> onResult,
                                    final Consumer<Exception> onFailure) {

        // wait for the configured number of active shards to be allocated before executing the result consumer
        if (activeShardCount == ActiveShardCount.NONE) {
            // not waiting, so just run whatever we were to run when the waiting is
            onResult.accept(true);
            return;
        }

        final ClusterState state = clusterService.state();
        final ClusterStateObserver observer = new ClusterStateObserver(
            state, clusterService.getClusterApplierService(), null, LOGGER);
        if (activeShardCount.enoughShardsActive(state, indexUUIDs)) {
            onResult.accept(true);
        } else {
            final Predicate<ClusterState> shardsAllocatedPredicate = newState -> activeShardCount.enoughShardsActive(newState, indexUUIDs);

            final ClusterStateObserver.Listener observerListener = new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    onResult.accept(true);
                }

                @Override
                public void onClusterServiceClose() {
                    LOGGER.debug("[{}] cluster service closed while waiting for enough shards to be started.", Arrays.toString(indexUUIDs));
                    onFailure.accept(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    onResult.accept(false);
                }
            };

            observer.waitForNextChange(observerListener, shardsAllocatedPredicate, timeout);
        }
    }

}

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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.BitSet;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class BulkActiveShardsObserver extends AbstractComponent {

    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public BulkActiveShardsObserver(final Settings settings, final ClusterService clusterService, final ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    /**
     * Waits on the specified number of active shards per given index to be started before executing
     *
     * @param indices          the indices to wait for active shards on
     * @param activeShardCount the number of active shards to wait on before returning per index
     * @param timeout          the timeout value
     * @param onResult         a function that is executed in response to the requisite shards becoming active or a timeout (whichever comes first)
     * @param onFailure        a function that is executed in response to an error occurring during waiting for the active shards
     */
    public void waitForActiveShards(Collection<String> indices,
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

        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());

        final BitSet activeIndices = new BitSet(indices.size());
        if (checkIndicesActiveShards(indices, activeIndices, activeShardCount, observer.setAndGetObservedState())) {
            onResult.accept(true);
            return;
        }

        final Predicate<ClusterState> shardsAllocatedPredicate =
            (clusterState) -> checkIndicesActiveShards(indices, activeIndices, activeShardCount, clusterState);

        final ClusterStateObserver.Listener observerListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                onResult.accept(true);
            }

            @Override
            public void onClusterServiceClose() {
                logger.debug("[{}] cluster service closed while waiting for enough shards to be started.", indices);
                onFailure.accept(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                onResult.accept(false);
            }
        };

        observer.waitForNextChange(observerListener, shardsAllocatedPredicate, timeout);
    }

    private boolean checkIndicesActiveShards(Collection<String> indices,
                                             BitSet activeIndices,
                                             ActiveShardCount activeShardCount,
                                             ClusterState clusterState) {
        int idx = 0;
        for (String indexName : indices) {
            if (activeIndices.get(idx) == false && activeShardCount.enoughShardsActive(clusterState, indexName)) {
                activeIndices.set(idx);
            }
            idx++;
        }
        return activeIndices.cardinality() == indices.size();
    }
}

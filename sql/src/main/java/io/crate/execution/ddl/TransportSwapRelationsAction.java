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

package io.crate.execution.ddl;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicReference;

public final class TransportSwapRelationsAction extends TransportMasterNodeAction<SwapRelationsRequest, AcknowledgedResponse> {

    private final SwapRelationsOperation swapRelationsOperation;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportSwapRelationsAction(Settings settings,
                                        TransportService transportService,
                                        ClusterService clusterService,
                                        ThreadPool threadPool,
                                        ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        AllocationService allocationService) {
        super(settings,
            "internal:crate:sql/alter/cluster/indices",
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            SwapRelationsRequest::new);
        this.activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
        this.swapRelationsOperation = new SwapRelationsOperation(allocationService, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(SwapRelationsRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        AtomicReference<String[]> indexNamesAfterRelationSwap = new AtomicReference<>(null);
        ActionListener<AcknowledgedResponse> waitForActiveShardsListener = ActionListener.wrap(response -> {
            if (response.isAcknowledged()) {
                activeShardsObserver.waitForActiveShards(
                    indexNamesAfterRelationSwap.get(),
                    ActiveShardCount.DEFAULT,
                    request.ackTimeout(),
                    shardsAcknowledged -> {
                        if (!shardsAcknowledged) {
                            logger.debug(
                                "Switched name of relations, but the operation timed out waiting for enough shards to be started");
                        }
                        listener.onResponse(response);
                    },
                    listener::onFailure
                );
            } else {
                listener.onResponse(response);
            }
        }, listener::onFailure);
        AckedClusterStateUpdateTask<AcknowledgedResponse> updateTask =
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.HIGH, request, waitForActiveShardsListener) {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    if (logger.isInfoEnabled()) {
                        Iterable<String> swapActions = request.swapActions().stream()
                            .map(x -> x.source().fqn() + " <-> " + x.target().fqn())
                            ::iterator;
                        logger.info("Swapping tables [{}]", String.join(", ", swapActions));
                    }
                    SwapRelationsOperation.UpdatedState newState = swapRelationsOperation.execute(currentState, request);
                    indexNamesAfterRelationSwap.set(newState.newIndices.toArray(new String[0]));
                    return newState.newState;
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }
            };
        clusterService.submitStateUpdateTask("swap-relations", updateTask);
    }

    @Override
    protected ClusterBlockException checkBlock(SwapRelationsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}

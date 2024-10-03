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

package io.crate.execution.ddl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.action.ActionListeners;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;

public final class TransportSwapRelationsAction extends TransportMasterNodeAction<SwapRelationsRequest, AcknowledgedResponse> {

    private final SwapRelationsOperation swapRelationsOperation;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportSwapRelationsAction(Settings settings,
                                        TransportService transportService,
                                        ClusterService clusterService,
                                        ThreadPool threadPool,
                                        DDLClusterStateService ddlClusterStateService,
                                        AllocationService allocationService) {
        super(
            "internal:crate:sql/alter/cluster/indices",
            transportService,
            clusterService,
            threadPool,
            SwapRelationsRequest::new
        );
        this.activeShardsObserver = new ActiveShardsObserver(clusterService);
        this.swapRelationsOperation = new SwapRelationsOperation(
            allocationService, ddlClusterStateService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(SwapRelationsRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        AtomicReference<String[]> indexNamesAfterRelationSwap = new AtomicReference<>(null);
        ActionListener<AcknowledgedResponse> waitForShardsListener = ActionListeners.waitForShards(
            listener,
            activeShardsObserver,
            request.ackTimeout(),
            () -> logger.info("Switched name of relations, but the operation timed out waiting for enough shards to be started"),
            indexNamesAfterRelationSwap::get
        );
        AckedClusterStateUpdateTask<AcknowledgedResponse> updateTask =
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.HIGH, request, waitForShardsListener) {

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
        Set<String> affectedIndices = new HashSet<>();
        Metadata metadata = state.metadata();
        for (RelationNameSwap swapAction : request.swapActions()) {
            affectedIndices.addAll(Arrays.asList(IndexNameExpressionResolver.concreteIndexNames(
                metadata, IndicesOptions.LENIENT_EXPAND_OPEN, swapAction.source().indexNameOrAlias())));
            affectedIndices.addAll(Arrays.asList(IndexNameExpressionResolver.concreteIndexNames(
                metadata, IndicesOptions.LENIENT_EXPAND_OPEN, swapAction.target().indexNameOrAlias())));
        }
        for (RelationName dropRelation : request.dropRelations()) {
            affectedIndices.addAll(Arrays.asList(IndexNameExpressionResolver.concreteIndexNames(
                metadata, IndicesOptions.LENIENT_EXPAND_OPEN, dropRelation.indexNameOrAlias())));
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, affectedIndices.toArray(new String[0]));
    }
}

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

package io.crate.execution.ddl.tables;

import io.crate.execution.support.ActionListeners;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.RenameTableClusterStateExecutor;
import io.crate.es.action.ActionListener;
import io.crate.es.action.support.ActiveShardsObserver;
import io.crate.es.action.support.IndicesOptions;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.action.support.master.TransportMasterNodeAction;
import io.crate.es.cluster.AckedClusterStateUpdateTask;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.routing.allocation.AllocationService;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.Priority;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.inject.Singleton;
import io.crate.es.common.settings.Settings;
import io.crate.es.index.IndexNotFoundException;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class TransportRenameTableAction extends TransportMasterNodeAction<RenameTableRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/rename";
    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);

    private final RenameTableClusterStateExecutor executor;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportRenameTableAction(Settings settings,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      AllocationService allocationService,
                                      DDLClusterStateService ddlClusterStateService) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool,
            indexNameExpressionResolver, RenameTableRequest::new);
        activeShardsObserver = new ActiveShardsObserver(settings, clusterService, threadPool);
        executor = new RenameTableClusterStateExecutor(
            indexNameExpressionResolver,
            allocationService,
            ddlClusterStateService
        );
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
    protected void masterOperation(RenameTableRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        AtomicReference<String[]> newIndexNames = new AtomicReference<>(null);
        ActionListener<AcknowledgedResponse> waitForShardsListener = ActionListeners.waitForShards(
            listener,
            activeShardsObserver,
            request.timeout(),
            () -> logger.info("Renamed a relation, but the operation timed out waiting for enough shards to become available"),
            newIndexNames::get
        );

        clusterService.submitStateUpdateTask(
            "rename-table",
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.HIGH, request, waitForShardsListener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState updatedState = executor.execute(currentState, request);
                    IndicesOptions openIndices = IndicesOptions.fromOptions(
                        true,
                        true,
                        true,
                        false,
                        true,
                        true,
                        false
                    );
                    newIndexNames.set(indexNameExpressionResolver.concreteIndexNames(
                            updatedState, openIndices, request.targetTableIdent().indexNameOrAlias()));
                    return updatedState;
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(RenameTableRequest request, ClusterState state) {
        try {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
                indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, request.sourceTableIdent().indexNameOrAlias()));
        } catch (IndexNotFoundException e) {
            if (request.isPartitioned() == false) {
                throw e;
            }
            // empty partition, no indices just a template exists.
            return null;
        }
    }
}

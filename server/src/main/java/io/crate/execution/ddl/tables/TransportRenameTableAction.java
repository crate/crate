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

package io.crate.execution.ddl.tables;

import java.io.IOException;
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
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.support.ActionListeners;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.RenameTableClusterStateExecutor;
import io.crate.metadata.view.ViewsMetadata;

@Singleton
public class TransportRenameTableAction extends TransportMasterNodeAction<RenameTableRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/rename";
    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);

    private final RenameTableClusterStateExecutor executor;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportRenameTableAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      AllocationService allocationService,
                                      DDLClusterStateService ddlClusterStateService) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              RenameTableRequest::new
        );
        activeShardsObserver = new ActiveShardsObserver(clusterService);
        executor = new RenameTableClusterStateExecutor(
            allocationService,
            ddlClusterStateService
        );
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
    protected void masterOperation(RenameTableRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
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
                        true
                    );
                    newIndexNames.set(IndexNameExpressionResolver.concreteIndexNames(
                        updatedState.metadata(), openIndices, request.targetName().indexNameOrAlias()));
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
        ViewsMetadata views = state.metadata().custom(ViewsMetadata.TYPE);
        if (views != null && views.contains(request.sourceName())) {
            return null;
        }
        try {
            return state.blocks().indicesBlockedException(
                ClusterBlockLevel.METADATA_WRITE,
                IndexNameExpressionResolver.concreteIndexNames(
                    state.metadata(),
                    STRICT_INDICES_OPTIONS,
                    request.sourceName().indexNameOrAlias()
                )
            );
        } catch (IndexNotFoundException e) {
            if (request.isPartitioned() == false) {
                throw e;
            }
            // empty partition, no indices just a template exists.
            return null;
        }
    }
}

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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActiveShardsObserver;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.action.ActionListeners;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.RenameTableClusterStateExecutor;
import io.crate.metadata.view.ViewsMetadata;

@Singleton
public class TransportRenameTable extends TransportMasterNodeAction<RenameTableRequest, AcknowledgedResponse> {

    public static final TransportRenameTable.Action ACTION = new TransportRenameTable.Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:sql/table/rename";

        private Action() {
            super(NAME);
        }
    }

    private final RenameTableClusterStateExecutor executor;
    private final ActiveShardsObserver activeShardsObserver;

    @Inject
    public TransportRenameTable(TransportService transportService,
                                ClusterService clusterService,
                                ThreadPool threadPool,
                                AllocationService allocationService,
                                DDLClusterStateService ddlClusterStateService) {
        super(ACTION.name(),
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
            new AckedClusterStateUpdateTask<>(Priority.HIGH, request, waitForShardsListener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState updatedState = executor.execute(currentState, request);
                    newIndexNames.set(updatedState.metadata().getIndices(
                        request.targetName(),
                        List.of(),
                        false,
                        imd -> imd.getState() == State.OPEN ? imd.getIndex().getName() : null
                    ).toArray(String[]::new));
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
        boolean strict = !request.isPartitioned();
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            state.metadata().getIndices(
                request.sourceName(),
                List.of(),
                strict,
                imd -> imd.getIndex().getName()
            ).toArray(String[]::new)
        );
    }
}

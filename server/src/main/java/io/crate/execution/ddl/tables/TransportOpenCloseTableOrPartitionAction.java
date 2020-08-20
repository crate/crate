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

import java.io.IOException;

import javax.annotation.Nullable;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.cluster.CloseTableClusterStateTaskExecutor;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;

@Singleton
public class TransportOpenCloseTableOrPartitionAction extends TransportMasterNodeAction<OpenCloseTableOrPartitionRequest, AcknowledgedResponse> {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    private static final String ACTION_NAME = "internal:crate:sql/table_or_partition/open_close";

    private final OpenTableClusterStateTaskExecutor openExecutor;
    private final CloseTableClusterStateTaskExecutor closeExecutor;

    @Inject
    public TransportOpenCloseTableOrPartitionAction(TransportService transportService,
                                                    ClusterService clusterService,
                                                    ThreadPool threadPool,
                                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                                    AllocationService allocationService,
                                                    DDLClusterStateService ddlClusterStateService,
                                                    MetadataIndexUpgradeService metadataIndexUpgradeService,
                                                    IndicesService indexServices) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            OpenCloseTableOrPartitionRequest::new,
            indexNameExpressionResolver
        );
        openExecutor = new OpenTableClusterStateTaskExecutor(
            indexNameExpressionResolver,
            allocationService,
            ddlClusterStateService,
            metadataIndexUpgradeService,
            indexServices
        );
        closeExecutor = new CloseTableClusterStateTaskExecutor(
            indexNameExpressionResolver,
            allocationService,
            ddlClusterStateService
        );
    }

    @Override
    protected ClusterBlockException checkBlock(OpenCloseTableOrPartitionRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, request.tableIdent().indexNameOrAlias())
        );
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(OpenCloseTableOrPartitionRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (request.isOpenTable()) {
            openTable(request, state, listener);
        } else {
            closeTable(request, state, listener);
        }
    }

    private void closeTable(OpenCloseTableOrPartitionRequest request,
                            ClusterState state,
                            ActionListener<AcknowledgedResponse> listener) throws Exception {
        clusterService.submitStateUpdateTask(
            "close-table-or-partition",
            request,
            ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
            closeExecutor,
            new AckedClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(new AcknowledgedResponse(false));
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            }
        );
    }

    private void openTable(OpenCloseTableOrPartitionRequest request,
                           ClusterState state,
                           ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask(
            "open-table-or-partition",
            request,
            ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
            openExecutor,
            new AckedClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(new AcknowledgedResponse(false));
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            }
        );
    }
}

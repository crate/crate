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

package io.crate.executor.transport.ddl;

import io.crate.metadata.cluster.ClusterStateTableOperations;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportOpenCloseTableOrPartitionAction extends TransportMasterNodeAction<OpenCloseTableOrPartitionRequest, OpenCloseTableOrPartitionResponse> {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    private static final String ACTION_NAME = "crate/sql/table_or_partition/open_close";

    private final ClusterStateTableOperations tableOperations;

    @Inject
    public TransportOpenCloseTableOrPartitionAction(Settings settings,
                                                    TransportService transportService,
                                                    ClusterService clusterService,
                                                    ThreadPool threadPool,
                                                    ActionFilters actionFilters,
                                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                                    ClusterStateTableOperations tableOperations) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, OpenCloseTableOrPartitionRequest::new);
        this.tableOperations = tableOperations;
    }


    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected OpenCloseTableOrPartitionResponse newResponse() {
        return new OpenCloseTableOrPartitionResponse();
    }

    @Override
    protected void masterOperation(OpenCloseTableOrPartitionRequest request,
                                   ClusterState state,
                                   ActionListener<OpenCloseTableOrPartitionResponse> listener) throws Exception {
        String source = "open-close-table-or-partition " + request.tableIdent() + ", open: " + request.isOpenTable();
        clusterService.submitStateUpdateTask(source,
            new AckedClusterStateUpdateTask<OpenCloseTableOrPartitionResponse>(Priority.URGENT, request, listener) {
                @Override
                protected OpenCloseTableOrPartitionResponse newResponse(boolean acknowledged) {
                    return new OpenCloseTableOrPartitionResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    if (request.isOpenTable()) {
                        return tableOperations.openTableOrPartition(currentState, request.tableIdent(),
                            request.partitionIndexName());
                    }
                    return tableOperations.closeTableOrPartition(currentState, request.tableIdent(),
                        request.partitionIndexName());
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(OpenCloseTableOrPartitionRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, request.tableIdent().indexName()));
    }
}

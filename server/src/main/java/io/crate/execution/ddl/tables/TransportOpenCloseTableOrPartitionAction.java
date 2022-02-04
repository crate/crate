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

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.CloseTableClusterStateTaskExecutor;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportOpenCloseTableOrPartitionAction extends AbstractDDLTransportAction<OpenCloseTableOrPartitionRequest, AcknowledgedResponse> {

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
                                                    OpenTableClusterStateTaskExecutor openExecutor) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            OpenCloseTableOrPartitionRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "open-table-or-partition");
        this.openExecutor = openExecutor;
        closeExecutor = new CloseTableClusterStateTaskExecutor(indexNameExpressionResolver, allocationService,
            ddlClusterStateService);
    }

    @Override
    public ClusterStateTaskExecutor<OpenCloseTableOrPartitionRequest> clusterStateTaskExecutor(OpenCloseTableOrPartitionRequest request) {
        if (request.isOpenTable()) {
            return openExecutor;
        } else {
            return closeExecutor;
        }
    }

    @Override
    protected ClusterBlockException checkBlock(OpenCloseTableOrPartitionRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, STRICT_INDICES_OPTIONS, request.tableIdent().indexNameOrAlias()));
    }
}

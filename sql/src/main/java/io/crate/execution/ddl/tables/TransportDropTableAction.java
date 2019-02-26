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

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.DropTableClusterStateTaskExecutor;
import io.crate.es.action.support.IndicesOptions;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.ClusterStateTaskExecutor;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.metadata.MetaDataDeleteIndexService;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.inject.Singleton;
import io.crate.es.common.settings.Settings;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

@Singleton
public class TransportDropTableAction extends AbstractDDLTransportAction<DropTableRequest, DropTableResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/drop";
    // Delete index should work by default on both open and closed indices.
    private static IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, true, true, true);

    private final DropTableClusterStateTaskExecutor executor;

    @Inject
    public TransportDropTableAction(Settings settings,
                                    TransportService transportService,
                                    ClusterService clusterService,
                                    ThreadPool threadPool,
                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                    MetaDataDeleteIndexService deleteIndexService,
                                    DDLClusterStateService ddlClusterStateService) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool,
            indexNameExpressionResolver, DropTableRequest::new, DropTableResponse::new, DropTableResponse::new, "drop-table");
        executor = new DropTableClusterStateTaskExecutor(indexNameExpressionResolver, deleteIndexService,
            ddlClusterStateService);
    }

    @Override
    public ClusterStateTaskExecutor<DropTableRequest> clusterStateTaskExecutor(DropTableRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(DropTableRequest request, ClusterState state) {
        IndicesOptions indicesOptions = INDICES_OPTIONS;
        if (request.isPartitioned()) {
            indicesOptions = IndicesOptions.lenientExpandOpen();
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, indicesOptions, request.tableIdent().indexNameOrAlias()));
    }
}

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

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.DropTableClusterStateTaskExecutor;

@Singleton
public class TransportDropTableAction extends AbstractDDLTransportAction<DropTableRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        public static final String NAME = "internal:crate:sql/table/drop";

        private Action() {
            super(NAME);
        }
    }

    // Delete index should work by default on both open and closed indices.
    private static final IndicesOptions INDICES_OPTIONS = IndicesOptions.fromOptions(false, true, true, true);

    private final DropTableClusterStateTaskExecutor executor;

    @Inject
    public TransportDropTableAction(TransportService transportService,
                                    ClusterService clusterService,
                                    ThreadPool threadPool,
                                    MetadataDeleteIndexService deleteIndexService,
                                    DDLClusterStateService ddlClusterStateService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            DropTableRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "drop-table"
        );
        executor = new DropTableClusterStateTaskExecutor(deleteIndexService, ddlClusterStateService);
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
            IndexNameExpressionResolver.concreteIndexNames(state.metadata(), indicesOptions, request.tableIdent().indexNameOrAlias()));
    }
}

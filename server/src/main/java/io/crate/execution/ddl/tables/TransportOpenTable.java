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

import static io.crate.execution.ddl.tables.TransportCloseTable.isEmptyPartitionedTable;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataUpgradeService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;

@Singleton
public class TransportOpenTable extends AbstractDDLTransportAction<OpenTableRequest, AcknowledgedResponse> {

    public static final TransportOpenTable.Action ACTION = new TransportOpenTable.Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:sql/table_or_partition/open_close";

        private Action() {
            super(NAME);
        }
    }

    private final OpenTableClusterStateTaskExecutor openExecutor;

    @Inject
    public TransportOpenTable(TransportService transportService,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              AllocationService allocationService,
                              DDLClusterStateService ddlClusterStateService,
                              MetadataUpgradeService metadataIndexUpgradeService,
                              IndicesService indexServices) {
        super(ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            OpenTableRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "open-table-or-partition");
        openExecutor = new OpenTableClusterStateTaskExecutor(
            allocationService,
            ddlClusterStateService,
            metadataIndexUpgradeService,
            indexServices
        );
    }

    @Override
    public ClusterStateTaskExecutor<OpenTableRequest> clusterStateTaskExecutor(OpenTableRequest request) {
        return openExecutor;
    }

    @Override
    protected ClusterBlockException checkBlock(OpenTableRequest request, ClusterState state) {
        RelationName relation = request.relation();
        if (isEmptyPartitionedTable(relation, state)) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
        String[] indexNames = state.metadata().getIndices(
            relation,
            request.partitionValues(),
            true,
            imd -> imd.getIndex().getName()
        ).toArray(String[]::new);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNames);
    }
}

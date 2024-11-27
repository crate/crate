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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataIndexUpgradeService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.OpenTableClusterStateTaskExecutor;

@Singleton
public class TransportOpenTableAction extends AbstractDDLTransportAction<OpenTableRequest, AcknowledgedResponse> {

    private static final IndicesOptions STRICT_INDICES_OPTIONS = IndicesOptions.fromOptions(false, false, false, false);
    private static final String ACTION_NAME = "internal:crate:sql/table_or_partition/open_close";

    private final OpenTableClusterStateTaskExecutor openExecutor;

    @Inject
    public TransportOpenTableAction(TransportService transportService,
                                                    ClusterService clusterService,
                                                    ThreadPool threadPool,
                                                    AllocationService allocationService,
                                                    DDLClusterStateService ddlClusterStateService,
                                                    MetadataIndexUpgradeService metadataIndexUpgradeService,
                                                    IndicesService indexServices) {
        super(ACTION_NAME,
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
        String[] indexNames;
        if (request.partitionValues().isEmpty()) {
            indexNames = IndexNameExpressionResolver.concreteIndexNames(
                state.metadata(),
                STRICT_INDICES_OPTIONS,
                relation.indexNameOrAlias()
            );
        } else {
            PartitionName partition = new PartitionName(relation, request.partitionValues());
            indexNames = IndexNameExpressionResolver.concreteIndexNames(
                state.metadata(),
                STRICT_INDICES_OPTIONS,
                partition.asIndexName()
            );
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNames);
    }
}

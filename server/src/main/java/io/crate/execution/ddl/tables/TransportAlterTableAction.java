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

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataIndexService;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.NodeContext;
import io.crate.metadata.cluster.AlterTableClusterStateExecutor;

@Singleton
public class TransportAlterTableAction extends AbstractDDLTransportAction<AlterTableRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/alter";

    private final AlterTableClusterStateExecutor executor;

    @Inject
    public TransportAlterTableAction(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool,
                                     IndicesService indicesService,
                                     IndexScopedSettings indexScopedSettings,
                                     MetadataIndexService metadataCreateIndexService,
                                     MetadataUpdateSettingsService updateSettingsService,
                                     NodeContext nodeContext) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              AlterTableRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
              "alter-table");
        executor = new AlterTableClusterStateExecutor(
            indicesService,
            indexScopedSettings,
            metadataCreateIndexService,
            updateSettingsService,
            nodeContext
        );
    }

    @Override
    public ClusterStateTaskExecutor<AlterTableRequest> clusterStateTaskExecutor(AlterTableRequest request) {
        return executor;
    }

    @Override
    public ClusterBlockException checkBlock(AlterTableRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

}

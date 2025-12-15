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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import io.crate.common.annotations.VisibleForTesting;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.NodeContext;

public class TransportDropColumn extends AbstractDDLTransportAction<DropColumnRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:sql/table/drop_column";

        private Action() {
            super(NAME);
        }
    }

    @VisibleForTesting
    public static final AlterTableTask.AlterTableOperator<DropColumnRequest> DROP_COLUMN_OPERATOR =
        (req, docTableInfo, _, _, _) -> docTableInfo.dropColumns(req.colsToDrop());
    private final NodeContext nodeContext;

    @Inject
    public TransportDropColumn(TransportService transportService,
                               ClusterService clusterService,
                               ThreadPool threadPool,
                               NodeContext nodeContext) {
        super(ACTION.name(),
              transportService,
              clusterService,
              threadPool,
              DropColumnRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
              "drop-column");
        this.nodeContext = nodeContext;
    }

    @Override
    public ClusterStateTaskExecutor<DropColumnRequest> clusterStateTaskExecutor(DropColumnRequest request) {
        return new AlterTableTask<>(nodeContext, request.relationName(), null, DROP_COLUMN_OPERATOR);
    }


    @Override
    public ClusterBlockException checkBlock(DropColumnRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}

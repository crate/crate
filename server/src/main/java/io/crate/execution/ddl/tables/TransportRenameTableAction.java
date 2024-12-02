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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.cluster.RenameTableClusterStateExecutor;

@Singleton
public class TransportRenameTableAction extends TransportMasterNodeAction<RenameTableRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/rename";
    private final RenameTableClusterStateExecutor executor;

    @Inject
    public TransportRenameTableAction(TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      DDLClusterStateService ddlClusterStateService) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              RenameTableRequest::new
        );
        executor = new RenameTableClusterStateExecutor(ddlClusterStateService);
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
        clusterService.submitStateUpdateTask(
            "rename-table",
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(Priority.HIGH, request, listener) {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return executor.execute(currentState, request);
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(RenameTableRequest request, ClusterState state) {
        return null;
    }
}

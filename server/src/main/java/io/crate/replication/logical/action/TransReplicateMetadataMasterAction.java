/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.action;

import io.crate.common.unit.TimeValue;
import io.crate.replication.logical.LogicalReplicationService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.function.Function;

public class TransReplicateMetadataMasterAction extends TransportMasterNodeAction<ReplicateMetadataRequest, AcknowledgedResponse> {

    private final LogicalReplicationService logicalReplicationService;

    public TransReplicateMetadataMasterAction(String actionName,
                                              TransportService transportService,
                                              ClusterService clusterService,
                                              ThreadPool threadPool,
                                              Writeable.Reader<ReplicateMetadataRequest> request,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              LogicalReplicationService logicalReplicationService) {
        super(actionName, transportService, clusterService, threadPool, request, indexNameExpressionResolver);
        this.logicalReplicationService = logicalReplicationService;

    }

    @Override
    protected String executor() {
        return null;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return null;
    }

    @Override
    protected void masterOperation(ReplicateMetadataRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        Client remoteClusterClient = logicalReplicationService.getRemoteClusterClient(threadPool, request.clusterName());
        // fetch from remote client metadata
        // check if something changed
        // update the cluster state
    }

    @Override
    protected ClusterBlockException checkBlock(ReplicateMetadataRequest request,
                                               ClusterState state) {
        return null;
    }
}

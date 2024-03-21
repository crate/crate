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

package io.crate.role;

import java.io.IOException;
import java.util.Locale;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.replication.logical.LogicalReplicationService;

public class TransportDropRoleAction extends TransportMasterNodeAction<DropRoleRequest, AcknowledgedResponse> {

    private final LogicalReplicationService logicalReplicationService;

    @Inject
    public TransportDropRoleAction(TransportService transportService,
                                   ClusterService clusterService,
                                   ThreadPool threadPool,
                                   LogicalReplicationService logicalReplicationService) {
        super(
            "internal:crate:sql/user/drop",
            transportService,
            clusterService,
            threadPool,
            DropRoleRequest::new
        );
        this.logicalReplicationService = logicalReplicationService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(DropRoleRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {

        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_6_0) == false) {
            throw new IllegalStateException("Cannot drop users/roles until all nodes are upgraded to 5.6");
        }

        String errorMsg = "User '%s' cannot be dropped. %s '%s' needs to be dropped first.";

        // Ensure user doesn't own subscriptions.
        logicalReplicationService
                .subscriptions()
                .forEach((key, value) -> {
                    if (value.owner().equals(request.roleName())) {
                        throw new IllegalStateException(
                                String.format(Locale.ENGLISH, errorMsg, request.roleName(), "Subscription", key)
                        );
                    }
                });

        // Ensure user doesn't own publications.
        logicalReplicationService
                .publications()
                .forEach((key, value) -> {
                    if (value.owner().equals(request.roleName())) {
                        throw new IllegalStateException(
                                String.format(Locale.ENGLISH, errorMsg, request.roleName(), "Publication", key)
                        );
                    }
                });

        DropRoleTask dropRoleTask = new DropRoleTask(request);
        dropRoleTask.completionFuture().whenComplete(listener);
        clusterService.submitStateUpdateTask("drop_role [" + request.roleName() + "]", dropRoleTask);
    }

    @Override
    protected ClusterBlockException checkBlock(DropRoleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}

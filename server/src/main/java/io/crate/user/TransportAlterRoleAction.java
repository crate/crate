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

package io.crate.user;

import java.io.IOException;

import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.user.metadata.UsersMetadata;

public class TransportAlterRoleAction extends TransportMasterNodeAction<AlterRoleRequest, WriteRoleResponse> {

    @Inject
    public TransportAlterRoleAction(TransportService transportService,
                                    ClusterService clusterService,
                                    ThreadPool threadPool) {
        super(
            "internal:crate:sql/user/alter",
            transportService,
            clusterService,
            threadPool,
            AlterRoleRequest::new
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WriteRoleResponse read(StreamInput in) throws IOException {
        return new WriteRoleResponse(in);
    }

    @Override
    protected void masterOperation(AlterRoleRequest request,
                                   ClusterState state,
                                   ActionListener<WriteRoleResponse> listener) {
        clusterService.submitStateUpdateTask("alter_role [" + request.roleName() + "]",
                new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {

                    private boolean userExists = true;

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Metadata currentMetadata = currentState.metadata();
                        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                        userExists = alterRole(
                                mdBuilder,
                                request.roleName(),
                                request.secureHash()
                        );
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }

                    @Override
                    protected WriteRoleResponse newResponse(boolean acknowledged) {
                        return new WriteRoleResponse(acknowledged, userExists);
                    }
                });
    }

    @VisibleForTesting
    static boolean alterRole(Metadata.Builder mdBuilder, String userName, @Nullable SecureHash secureHash) {
        UsersMetadata oldMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        if (oldMetadata == null || !oldMetadata.contains(userName)) {
            return false;
        }
        // create a new instance of the metadata, to guarantee the cluster changed action.
        UsersMetadata newMetadata = UsersMetadata.newInstance(oldMetadata);
        newMetadata.put(userName, secureHash);

        assert !newMetadata.equals(oldMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(UsersMetadata.TYPE, newMetadata);

        return true;
    }

    @Override
    protected ClusterBlockException checkBlock(AlterRoleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}

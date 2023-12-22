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
import java.util.Set;

import org.elasticsearch.Version;
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
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class TransportCreateRoleAction extends TransportMasterNodeAction<CreateRoleRequest, WriteRoleResponse> {

    @Inject
    public TransportCreateRoleAction(TransportService transportService,
                                     ClusterService clusterService,
                                     ThreadPool threadPool) {
        super(
            "internal:crate:sql/user/create",
            transportService,
            clusterService,
            threadPool,
            CreateRoleRequest::new
        );
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected WriteRoleResponse read(StreamInput in) throws IOException {
        return new WriteRoleResponse(in);
    }

    @Override
    protected void masterOperation(CreateRoleRequest request,
                                   ClusterState state,
                                   ActionListener<WriteRoleResponse> listener) throws Exception {
        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_6_0) == false) {
            throw new IllegalStateException("Cannot create new users/roles until all nodes are upgraded to 5.6");
        }

        clusterService.submitStateUpdateTask("create_role [" + request.roleName() + "]",
                new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {

                    private boolean alreadyExists = false;

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Metadata currentMetadata = currentState.metadata();
                        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                        alreadyExists = putRole(mdBuilder, request.roleName(), request.isUser(), request.secureHash());
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }

                    @Override
                    protected WriteRoleResponse newResponse(boolean acknowledged) {
                        return new WriteRoleResponse(acknowledged, alreadyExists);
                    }
                });
    }

    @Override
    protected ClusterBlockException checkBlock(CreateRoleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    /**
     * Puts a user into the meta data and creates an empty privileges set.
     *
     * @return boolean true if the user already exists, otherwise false
     */
    @VisibleForTesting
    static boolean putRole(Metadata.Builder mdBuilder, String roleName, boolean isUser, @Nullable SecureHash secureHash) {
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        UsersMetadata oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);

        UsersPrivilegesMetadata oldUserPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        boolean exists = true;
        if (newMetadata.contains(roleName) == false) {
            newMetadata.roles().put(roleName, new Role(roleName, isUser, Set.of(), secureHash, Set.of()));
            exists = false;
        } else if (newMetadata.equals(oldRolesMetadata)) {
            // nothing changed, no need to update the cluster state
            return exists;
        }

        assert !newMetadata.equals(oldRolesMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);
        return exists;
    }
}

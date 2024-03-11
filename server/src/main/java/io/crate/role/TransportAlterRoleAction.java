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

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

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
        if (state.nodes().getMinNodeVersion().onOrAfter(Version.V_5_6_0) == false) {
            throw new IllegalStateException("Cannot alter users/roles until all nodes are upgraded to 5.6");
        }

        clusterService.submitStateUpdateTask("alter_role [" + request.roleName() + "]",
                new AckedClusterStateUpdateTask<>(Priority.IMMEDIATE, request, listener) {

                    private boolean roleExists = true;

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Metadata currentMetadata = currentState.metadata();
                        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
                        roleExists = alterRole(
                                mdBuilder,
                                request.roleName(),
                                request.secureHash(),
                                request.jwtProperties(),
                                request.resetPassword(),
                                request.resetJwtProperties()
                        );
                        return ClusterState.builder(currentState).metadata(mdBuilder).build();
                    }

                    @Override
                    protected WriteRoleResponse newResponse(boolean acknowledged) {
                        return new WriteRoleResponse(acknowledged, roleExists);
                    }
                });
    }

    @VisibleForTesting
    static boolean alterRole(Metadata.Builder mdBuilder,
                             String roleName,
                             @Nullable SecureHash secureHash,
                             @Nullable JwtProperties jwtProperties,
                             boolean resetPassword,
                             boolean resetJwtProperties) {
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        UsersMetadata oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        if (oldUsersMetadata == null && oldRolesMetadata == null) {
            return false;
        }

        UsersPrivilegesMetadata oldUserPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        boolean exists = false;
        var role = newMetadata.roles().get(roleName);
        if (role != null) {
            if (role.isUser() == false) {
                if (secureHash != null || resetPassword) {
                    throw new UnsupportedFeatureException("Setting a password to a ROLE is not allowed");
                }
                if (jwtProperties != null || resetJwtProperties) {
                    throw new UnsupportedFeatureException("Setting JWT properties to a ROLE is not allowed");
                }
            }

            var newSecureHash = secureHash != null ? secureHash : (resetPassword ? null : role.password());
            var newJwtProperties = jwtProperties != null ? jwtProperties : (resetJwtProperties ? null : role.jwtProperties());

            if (newMetadata.contains(newJwtProperties)) {
                throw new RoleAlreadyExistsException(
                    "Another role with the same combination of iss/username jwt properties already exists"
                );
            }

            newMetadata.roles().put(roleName, role.with(newSecureHash, newJwtProperties));
            exists = true;
        }
        if (newMetadata.equals(oldRolesMetadata)) {
            return exists;
        }

        assert !newMetadata.equals(oldRolesMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);

        return exists;
    }

    @Override
    protected ClusterBlockException checkBlock(AlterRoleRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}

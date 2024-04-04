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

package io.crate.role;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class AlterRoleTask extends AckedClusterStateUpdateTask<WriteRoleResponse> {

    private final AlterRoleRequest request;
    private boolean isAltered;

    protected AlterRoleTask(AlterRoleRequest request) {
        super(Priority.IMMEDIATE, request);
        this.request = request;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
        isAltered = alterRole(
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
        return new WriteRoleResponse(acknowledged, isAltered);
    }

    private static boolean alterRole(Metadata.Builder mdBuilder,
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
        boolean isAltered = false;
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
            isAltered = true;
        }
        if (newMetadata.equals(oldRolesMetadata)) {
            return isAltered;
        }

        assert !newMetadata.equals(oldRolesMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);

        return isAltered;
    }
}

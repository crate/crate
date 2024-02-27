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

import java.util.Collection;

import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class DropRoleTask extends AckedClusterStateUpdateTask<WriteRoleResponse> {

    private final DropRoleRequest request;
    private boolean alreadyExists = true;

    DropRoleTask(DropRoleRequest request) {
        super(Priority.URGENT, request);
        this.request = request;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
        alreadyExists = dropRole(
            mdBuilder,
            request.roleName()
        );
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    @Override
    protected WriteRoleResponse newResponse(boolean acknowledged) {
        return new WriteRoleResponse(acknowledged, alreadyExists);
    }

    @VisibleForTesting
    static boolean dropRole(Metadata.Builder mdBuilder, String roleNameToDrop) {
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        UsersMetadata oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);
        if (oldUsersMetadata == null && oldRolesMetadata == null) {
            return false;
        }

        UsersPrivilegesMetadata oldUserPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        validateHasChildren(newMetadata.roles().values(), roleNameToDrop);
        var role = newMetadata.remove(roleNameToDrop);
        if (role == null && newMetadata.equals(oldRolesMetadata)) {
            return false;
        }

        assert !newMetadata.equals(oldRolesMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);

        return role != null;
    }

    private static void validateHasChildren(Collection<Role> roles, String roleNameToDrop) {
        for (Role role : roles) {
            if (role.grantedRoleNames().contains(roleNameToDrop)) {
                throw new IllegalArgumentException(
                    "Cannot drop ROLE: " + roleNameToDrop + " as it is granted on role: " + role.name());
            }
        }
    }
}

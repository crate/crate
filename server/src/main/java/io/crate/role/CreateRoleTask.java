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

import java.util.Set;

import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;
import org.jetbrains.annotations.Nullable;

import io.crate.common.annotations.VisibleForTesting;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class CreateRoleTask extends AckedClusterStateUpdateTask<WriteRoleResponse> {

    private final CreateRoleRequest request;
    private boolean alreadyExists = false;

    protected CreateRoleTask(CreateRoleRequest request) {
        super(Priority.IMMEDIATE, request);
        this.request = request;
    }

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
            newMetadata.roles().put(roleName, new Role(roleName, isUser, Set.of(), Set.of(), secureHash));
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

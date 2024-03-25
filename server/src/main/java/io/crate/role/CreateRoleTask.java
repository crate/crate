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

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Priority;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.role.metadata.RolesMetadata;
import io.crate.role.metadata.UsersMetadata;
import io.crate.role.metadata.UsersPrivilegesMetadata;

public class CreateRoleTask extends AckedClusterStateUpdateTask<AcknowledgedResponse> {
    private final CreateRoleRequest request;
    private boolean isCreated;

    protected CreateRoleTask(CreateRoleRequest request) {
        super(Priority.IMMEDIATE, request);
        this.request = request;
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder mdBuilder = Metadata.builder(currentMetadata);
        isCreated = createRole(
            mdBuilder,
            request.roleName(),
            request.isUser(),
            request.secureHash(),
            request.jwtProperties()
        );
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    @Override
    protected AcknowledgedResponse newResponse(boolean acknowledged) {
        return new AcknowledgedResponse(acknowledged && isCreated);
    }

    /**
     * Puts a user into the meta data and creates an empty privileges set.
     *
     * @return boolean true if the user already exists, otherwise false
     */
    @VisibleForTesting
    static boolean createRole(Metadata.Builder mdBuilder,
                              String roleName,
                              boolean isUser,
                              @Nullable SecureHash secureHash,
                              @Nullable JwtProperties jwtProperties) {
        RolesMetadata oldRolesMetadata = (RolesMetadata) mdBuilder.getCustom(RolesMetadata.TYPE);
        UsersMetadata oldUsersMetadata = (UsersMetadata) mdBuilder.getCustom(UsersMetadata.TYPE);

        UsersPrivilegesMetadata oldUserPrivilegesMetadata = (UsersPrivilegesMetadata) mdBuilder.getCustom(UsersPrivilegesMetadata.TYPE);
        RolesMetadata newMetadata = RolesMetadata.of(mdBuilder, oldUsersMetadata, oldUserPrivilegesMetadata, oldRolesMetadata);
        boolean isCreated = false;
        if (newMetadata.contains(jwtProperties)) {
            throw new RoleAlreadyExistsException(
                "Another role with the same combination of iss/username jwt properties already exists"
            );
        }
        if (newMetadata.contains(roleName) == false) {
            newMetadata.roles().put(roleName, new Role(roleName, isUser, Set.of(), Set.of(), secureHash, jwtProperties));
            isCreated = true;
        } else if (newMetadata.equals(oldRolesMetadata)) {
            // nothing changed, no need to update the cluster state
            return false;
        }

        assert !newMetadata.equals(oldRolesMetadata) : "must not be equal to guarantee the cluster change action";
        mdBuilder.putCustom(RolesMetadata.TYPE, newMetadata);
        return isCreated;
    }
}

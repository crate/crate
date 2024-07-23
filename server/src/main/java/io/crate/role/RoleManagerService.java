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

import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.auth.AccessControlImpl;
import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.RoleUnknownException;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.settings.CoordinatorSessionSettings;

@Singleton
public class RoleManagerService implements RoleManager {

    private static void ensureDropRoleTargetIsNotSuperUser(Role user) {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot drop a superuser '%s'", user.name()));
        }
    }

    private static void ensureAlterPrivilegeTargetIsNotSuperuser(Role user) {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot alter privileges for superuser '%s'", user.name()));
        }
    }


    private static final RoleManagerDDLModifier DDL_MODIFIER = new RoleManagerDDLModifier();

    private final Roles roles;
    private final NodeClient client;

    public RoleManagerService(NodeClient client,
                              Roles roles,
                              DDLClusterStateService ddlClusterStateService) {
        this.client = client;
        this.roles = roles;
        ddlClusterStateService.addModifier(DDL_MODIFIER);
    }


    @Override
    public CompletableFuture<Long> createRole(String roleName,
                                              boolean isUser,
                                              @Nullable SecureHash hashedPw,
                                              @Nullable JwtProperties jwtProperties) {
        CreateRoleRequest request = new CreateRoleRequest(roleName, isUser, hashedPw, jwtProperties);
        return client.execute(TransportCreateRoleAction.ACTION, request).thenApply(r -> {
            if (r.doesUserExist()) {
                throw new RoleAlreadyExistsException(String.format(Locale.ENGLISH, "Role '%s' already exists", roleName));
            }
            return 1L;
        });
    }

    @Override
    public CompletableFuture<Long> dropRole(String roleName, boolean suppressNotFoundError) {
        ensureDropRoleTargetIsNotSuperUser(roles.findUser(roleName));
        DropRoleRequest request = new DropRoleRequest(roleName, suppressNotFoundError);
        return client.execute(TransportDropRoleAction.ACTION, request).thenApply(r -> {
            if (r.doesUserExist() == false) {
                if (suppressNotFoundError) {
                    return 0L;
                }
                throw new RoleUnknownException(roleName);
            }
            return 1L;
        });
    }

    @Override
    public CompletableFuture<Long> alterRole(String roleName,
                                             @Nullable SecureHash newHashedPw,
                                             @Nullable JwtProperties newJwtProperties,
                                             boolean resetPassword,
                                             boolean resetJwtProperties) {
        AlterRoleRequest request = new AlterRoleRequest(
            roleName,
            newHashedPw,
            newJwtProperties,
            resetPassword,
            resetJwtProperties
        );
        return client.execute(TransportAlterRoleAction.ACTION, request).thenApply(r -> {
            if (r.doesUserExist() == false) {
                throw new RoleUnknownException(roleName);
            }
            return 1L;
        });
    }

    public CompletableFuture<Long> applyPrivileges(Collection<String> roleNames,
                                                   Collection<Privilege> privileges,
                                                   GrantedRolesChange grantedRolesChange) {
        roleNames.forEach(s -> ensureAlterPrivilegeTargetIsNotSuperuser(roles.findUser(s)));
        PrivilegesRequest request = new PrivilegesRequest(roleNames, privileges, grantedRolesChange);
        return client.execute(TransportPrivilegesAction.ACTION, request).thenApply(r -> {
            if (!r.unknownUserNames().isEmpty()) {
                throw new RoleUnknownException(r.unknownUserNames());
            }
            return r.affectedRows();
        });
    }


    @Override
    public AccessControl getAccessControl(CoordinatorSessionSettings sessionSettings) {
        return new AccessControlImpl(roles, sessionSettings);
    }

    public Collection<Role> roles() {
        return roles.roles();
    }
}

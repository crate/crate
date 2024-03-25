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

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.auth.AccessControlImpl;
import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.RoleUnknownException;
import io.crate.execution.engine.collect.sources.SysTableRegistry;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.role.metadata.SysPrivilegesTableInfo;
import io.crate.role.metadata.SysRolesTableInfo;
import io.crate.role.metadata.SysUsersTableInfo;

@Singleton
public class RoleManagerService implements RoleManager {

    private static final void ensureDropRoleTargetIsNotSuperUser(Role user) {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot drop a superuser '%s'", user.name()));
        }
    }

    private static final void ensureAlterPrivilegeTargetIsNotSuperuser(Role user) {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot alter privileges for superuser '%s'", user.name()));
        }
    }


    private static final RoleManagerDDLModifier DDL_MODIFIER = new RoleManagerDDLModifier();

    private final TransportCreateRoleAction transportCreateRoleAction;
    private final TransportDropRoleAction transportDropRoleAction;
    private final TransportAlterRoleAction transportAlterRoleAction;
    private final TransportPrivilegesAction transportPrivilegesAction;

    private final Roles roles;

    @Inject
    public RoleManagerService(TransportCreateRoleAction transportCreateRoleAction,
                              TransportDropRoleAction transportDropRoleAction,
                              TransportAlterRoleAction transportAlterRoleAction,
                              TransportPrivilegesAction transportPrivilegesAction,
                              SysTableRegistry sysTableRegistry,
                              Roles roles,
                              DDLClusterStateService ddlClusterStateService,
                              ClusterService clusterService) {
        this.transportCreateRoleAction = transportCreateRoleAction;
        this.transportDropRoleAction = transportDropRoleAction;
        this.transportAlterRoleAction = transportAlterRoleAction;
        this.transportPrivilegesAction = transportPrivilegesAction;
        this.roles = roles;
        var userTable = SysUsersTableInfo.create(() -> clusterService.state().metadata().clusterUUID());
        sysTableRegistry.registerSysTable(
            userTable,
            () -> CompletableFuture.completedFuture(
                roles.roles().stream().filter(Role::isUser).toList()),
            userTable.expressions(),
            false
        );
        var rolesTable = SysRolesTableInfo.create();
        sysTableRegistry.registerSysTable(
            rolesTable,
            () -> CompletableFuture.completedFuture(
                roles.roles().stream().filter(r -> r.isUser() == false).toList()),
                rolesTable.expressions(),
            false
        );

        var privilegesTable = SysPrivilegesTableInfo.create();
        sysTableRegistry.registerSysTable(
            privilegesTable,
            () -> CompletableFuture.completedFuture(SysPrivilegesTableInfo.buildPrivilegesRows(roles.roles())),
            privilegesTable.expressions(),
            false
        );

        ddlClusterStateService.addModifier(DDL_MODIFIER);
    }


    @Override
    public CompletableFuture<Long> createRole(String roleName,
                                              boolean isUser,
                                              @Nullable SecureHash hashedPw,
                                              @Nullable JwtProperties jwtProperties) {
        return transportCreateRoleAction.execute(new CreateRoleRequest(roleName, isUser, hashedPw, jwtProperties), r -> {
            if (r.isAcknowledged()) {
                throw new RoleAlreadyExistsException(String.format(Locale.ENGLISH, "Role '%s' already exists", roleName));
            }
            return 1L;
        });
    }

    @Override
    public CompletableFuture<Long> dropRole(String roleName, boolean suppressNotFoundError) {
        ensureDropRoleTargetIsNotSuperUser(roles.findUser(roleName));
        return transportDropRoleAction.execute(new DropRoleRequest(roleName, suppressNotFoundError), r -> {
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
        return transportAlterRoleAction.execute(
            new AlterRoleRequest(roleName, newHashedPw, newJwtProperties, resetPassword, resetJwtProperties),
            r -> {
                if (r.doesUserExist() == false) {
                    throw new RoleUnknownException(roleName);
                }
                return 1L;
            }
        );
    }

    public CompletableFuture<Long> applyPrivileges(Collection<String> roleNames,
                                                   Collection<Privilege> privileges,
                                                   GrantedRolesChange grantedRolesChange) {
        roleNames.forEach(s -> ensureAlterPrivilegeTargetIsNotSuperuser(roles.findUser(s)));
        return transportPrivilegesAction.execute(new PrivilegesRequest(roleNames, privileges, grantedRolesChange), r -> {
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

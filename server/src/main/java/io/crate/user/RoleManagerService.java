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

import java.util.Collection;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
import io.crate.user.metadata.SysPrivilegesTableInfo;
import io.crate.user.metadata.SysRolesTableInfo;
import io.crate.user.metadata.SysUsersTableInfo;

@Singleton
public class RoleManagerService implements RoleManager {

    private static final Consumer<Role> ENSURE_DROP_ROLE_NOT_SUPERUSER = user -> {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot drop a superuser '%s'", user.name()));
        }
    };

    private static final Consumer<Role> ENSURE_PRIVILEGE_USER_NOT_SUPERUSER = user -> {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot alter privileges for superuser '%s'", user.name()));
        }
    };

    private static final RoleManagerDDLModifier DDL_MODIFIER = new RoleManagerDDLModifier();

    private final TransportCreateRoleAction transportCreateRoleAction;
    private final TransportDropRoleAction transportDropRoleAction;
    private final TransportAlterRoleAction transportAlterRoleAction;
    private final TransportPrivilegesAction transportPrivilegesAction;

    private final RoleLookup roleLookup;

    @Inject
    public RoleManagerService(TransportCreateRoleAction transportCreateRoleAction,
                              TransportDropRoleAction transportDropRoleAction,
                              TransportAlterRoleAction transportAlterRoleAction,
                              TransportPrivilegesAction transportPrivilegesAction,
                              SysTableRegistry sysTableRegistry,
                              RoleLookup roleLookup,
                              DDLClusterStateService ddlClusterStateService) {
        this.transportCreateRoleAction = transportCreateRoleAction;
        this.transportDropRoleAction = transportDropRoleAction;
        this.transportAlterRoleAction = transportAlterRoleAction;
        this.transportPrivilegesAction = transportPrivilegesAction;
        this.roleLookup = roleLookup;
        var userTable = SysUsersTableInfo.create();
        sysTableRegistry.registerSysTable(
            userTable,
            () -> CompletableFuture.completedFuture(
                roleLookup.roles().stream().filter(Role::isUser).toList()),
            userTable.expressions(),
            false
        );
        var rolesTable = SysRolesTableInfo.create();
        sysTableRegistry.registerSysTable(
            rolesTable,
            () -> CompletableFuture.completedFuture(
                roleLookup.roles().stream().filter(r -> r.isUser() == false).toList()),
                rolesTable.expressions(),
            false
        );

        var privilegesTable = SysPrivilegesTableInfo.create();
        sysTableRegistry.registerSysTable(
            privilegesTable,
            () -> CompletableFuture.completedFuture(SysPrivilegesTableInfo.buildPrivilegesRows(roleLookup.roles())),
            privilegesTable.expressions(),
            false
        );

        ddlClusterStateService.addModifier(DDL_MODIFIER);
    }


    @Override
    public CompletableFuture<Long> createRole(String roleName, boolean isUser, @Nullable SecureHash hashedPw) {
        return transportCreateRoleAction.execute(new CreateRoleRequest(roleName, isUser, hashedPw), r -> {
            if (r.doesUserExist()) {
                throw new RoleAlreadyExistsException(roleName);
            }
            return 1L;
        });
    }

    @Override
    public CompletableFuture<Long> dropRole(String roleName, boolean suppressNotFoundError) {
        ENSURE_DROP_ROLE_NOT_SUPERUSER.accept(roleLookup.findUser(roleName));
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
    public CompletableFuture<Long> alterRole(String roleName, @Nullable SecureHash newHashedPw) {
        return transportAlterRoleAction.execute(new AlterRoleRequest(roleName, newHashedPw), r -> {
            if (r.doesUserExist() == false) {
                throw new RoleUnknownException(roleName);
            }
            return 1L;
        });
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> roleNames, Collection<Privilege> privileges) {
        roleNames.forEach(s -> ENSURE_PRIVILEGE_USER_NOT_SUPERUSER.accept(roleLookup.findUser(s)));
        return transportPrivilegesAction.execute(new PrivilegesRequest(roleNames, privileges), r -> {
            if (!r.unknownUserNames().isEmpty()) {
                throw new RoleUnknownException(r.unknownUserNames());
            }
            return r.affectedRows();
        });
    }


    @Override
    public AccessControl getAccessControl(CoordinatorSessionSettings sessionSettings) {
        return new AccessControlImpl(roleLookup, sessionSettings);
    }

    public Collection<Role> roles() {
        return roleLookup.roles();
    }
}

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

import javax.annotation.Nullable;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import io.crate.action.FutureActionListener;
import io.crate.action.sql.SessionContext;
import io.crate.auth.AccessControl;
import io.crate.auth.AccessControlImpl;
import io.crate.exceptions.UserAlreadyExistsException;
import io.crate.exceptions.UserUnknownException;
import io.crate.execution.engine.collect.sources.SysTableRegistry;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.user.metadata.SysPrivilegesTableInfo;
import io.crate.user.metadata.SysUsersTableInfo;

@Singleton
public class UserManagerService implements UserManager {

    private static final Consumer<User> ENSURE_DROP_USER_NOT_SUPERUSER = user -> {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot drop a superuser '%s'", user.name()));
        }
    };

    private static final Consumer<User> ENSURE_PRIVILEGE_USER_NOT_SUPERUSER = user -> {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot alter privileges for superuser '%s'", user.name()));
        }
    };

    private static final UserManagerDDLModifier DDL_MODIFIER = new UserManagerDDLModifier();

    private final TransportCreateUserAction transportCreateUserAction;
    private final TransportDropUserAction transportDropUserAction;
    private final TransportAlterUserAction transportAlterUserAction;
    private final TransportPrivilegesAction transportPrivilegesAction;

    private final UserLookup userLookup;

    @Inject
    public UserManagerService(TransportCreateUserAction transportCreateUserAction,
                              TransportDropUserAction transportDropUserAction,
                              TransportAlterUserAction transportAlterUserAction,
                              TransportPrivilegesAction transportPrivilegesAction,
                              SysTableRegistry sysTableRegistry,
                              ClusterService clusterService,
                              UserLookup userLookup,
                              DDLClusterStateService ddlClusterStateService) {
        this.transportCreateUserAction = transportCreateUserAction;
        this.transportDropUserAction = transportDropUserAction;
        this.transportAlterUserAction = transportAlterUserAction;
        this.transportPrivilegesAction = transportPrivilegesAction;
        this.userLookup = userLookup;
        var userTable = SysUsersTableInfo.create();
        sysTableRegistry.registerSysTable(
            userTable,
            () -> CompletableFuture.completedFuture(userLookup.users()),
            userTable.expressions(),
            false
        );

        var privilegesTable = SysPrivilegesTableInfo.create();
        sysTableRegistry.registerSysTable(
            privilegesTable,
            () -> CompletableFuture.completedFuture(SysPrivilegesTableInfo.buildPrivilegesRows(userLookup.users())),
            privilegesTable.expressions(),
            false
        );

        ddlClusterStateService.addModifier(DDL_MODIFIER);
    }


    @Override
    public CompletableFuture<Long> createUser(String userName, @Nullable SecureHash hashedPw) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> {
            if (r.doesUserExist()) {
                throw new UserAlreadyExistsException(userName);
            }
            return 1L;
        });
        transportCreateUserAction.execute(new CreateUserRequest(userName, hashedPw), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> dropUser(String userName, boolean suppressNotFoundError) {
        ENSURE_DROP_USER_NOT_SUPERUSER.accept(userLookup.findUser(userName));
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> {
            if (r.doesUserExist() == false) {
                if (suppressNotFoundError) {
                    return 0L;
                }
                throw new UserUnknownException(userName);
            }
            return 1L;
        });
        transportDropUserAction.execute(new DropUserRequest(userName, suppressNotFoundError), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> alterUser(String userName, @Nullable SecureHash newHashedPw) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> {
            if (r.doesUserExist() == false) {
                throw new UserUnknownException(userName);
            }
            return 1L;
        });
        transportAlterUserAction.execute(new AlterUserRequest(userName, newHashedPw), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> userNames, Collection<Privilege> privileges) {
        userNames.forEach(s -> ENSURE_PRIVILEGE_USER_NOT_SUPERUSER.accept(userLookup.findUser(s)));
        FutureActionListener<PrivilegesResponse, Long> listener = new FutureActionListener<>(r -> {
            //noinspection PointlessBooleanExpression
            if (r.unknownUserNames().isEmpty() == false) {
                throw new UserUnknownException(r.unknownUserNames());
            }
            return r.affectedRows();
        });
        transportPrivilegesAction.execute(new PrivilegesRequest(userNames, privileges), listener);
        return listener;
    }


    @Override
    public AccessControl getAccessControl(SessionContext sessionContext) {
        return new AccessControlImpl(userLookup, sessionContext);
    }


    @Override
    public boolean isEnabled() {
        return true;
    }

    public User findUser(String userName) {
        return userLookup.findUser(userName);
    }


    public Iterable<User> users() {
        return userLookup.users();
    }
}

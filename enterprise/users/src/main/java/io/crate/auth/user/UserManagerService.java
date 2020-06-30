/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.auth.user;

import com.google.common.collect.ImmutableSet;
import io.crate.action.FutureActionListener;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.UserAlreadyExistsException;
import io.crate.exceptions.UserUnknownException;
import io.crate.execution.engine.collect.sources.SysTableRegistry;
import io.crate.metadata.UsersMetadata;
import io.crate.metadata.UsersPrivilegesMetadata;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.sys.SysPrivilegesTableInfo;
import io.crate.metadata.sys.SysUsersTableInfo;
import io.crate.user.SecureHash;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.crate.auth.user.User.CRATE_USER;

@Singleton
public class UserManagerService implements UserManager, ClusterStateListener {

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
    private volatile Set<User> users = ImmutableSet.of(CRATE_USER);

    @Inject
    public UserManagerService(TransportCreateUserAction transportCreateUserAction,
                              TransportDropUserAction transportDropUserAction,
                              TransportAlterUserAction transportAlterUserAction,
                              TransportPrivilegesAction transportPrivilegesAction,
                              SysTableRegistry sysTableRegistry,
                              ClusterService clusterService,
                              DDLClusterStateService ddlClusterStateService) {
        this.transportCreateUserAction = transportCreateUserAction;
        this.transportDropUserAction = transportDropUserAction;
        this.transportAlterUserAction = transportAlterUserAction;
        this.transportPrivilegesAction = transportPrivilegesAction;
        clusterService.addListener(this);
        var userTable = SysUsersTableInfo.create();
        sysTableRegistry.registerSysTable(
            userTable,
            () -> CompletableFuture.completedFuture(users()),
            userTable.expressions(),
            false
        );

        var privilegesTable = SysPrivilegesTableInfo.create();
        sysTableRegistry.registerSysTable(
            privilegesTable,
            () -> CompletableFuture.completedFuture(SysPrivilegesTableInfo.buildPrivilegesRows(users())),
            privilegesTable.expressions(),
            false
        );

        ddlClusterStateService.addModifier(DDL_MODIFIER);
    }

    static Set<User> getUsers(@Nullable UsersMetadata metadata,
                              @Nullable UsersPrivilegesMetadata privilegesMetadata) {
        ImmutableSet.Builder<User> usersBuilder = new ImmutableSet.Builder<User>().add(CRATE_USER);
        if (metadata != null) {
            for (Map.Entry<String, SecureHash> user: metadata.users().entrySet()) {
                String userName = user.getKey();
                SecureHash password = user.getValue();
                Set<Privilege> privileges = null;
                if (privilegesMetadata != null) {
                    privileges = privilegesMetadata.getUserPrivileges(userName);
                }
                usersBuilder.add(User.of(userName, privileges, password));
            }
        }
        return usersBuilder.build();
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
        ENSURE_DROP_USER_NOT_SUPERUSER.accept(findUser(userName));
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
        userNames.forEach(s -> ENSURE_PRIVILEGE_USER_NOT_SUPERUSER.accept(findUser(s)));
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

    public Iterable<User> users() {
        return users;
    }

    @Override
    public AccessControl getAccessControl(SessionContext sessionContext) {
        return new AccessControlImpl(this, sessionContext);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Metadata prevMetadata = event.previousState().metadata();
        Metadata newMetadata = event.state().metadata();

        UsersMetadata prevUsers = prevMetadata.custom(UsersMetadata.TYPE);
        UsersMetadata newUsers = newMetadata.custom(UsersMetadata.TYPE);

        UsersPrivilegesMetadata prevUsersPrivileges = prevMetadata.custom(UsersPrivilegesMetadata.TYPE);
        UsersPrivilegesMetadata newUsersPrivileges = newMetadata.custom(UsersPrivilegesMetadata.TYPE);

        if (prevUsers != newUsers || prevUsersPrivileges != newUsersPrivileges) {
            users = getUsers(newUsers, newUsersPrivileges);
        }
    }


    @Nullable
    public User findUser(String userName) {
        for (User user : users()) {
            if (userName.equals(user.name())) {
                return user;
            }
        }
        return null;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}

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

package io.crate.operation.user;

import com.google.common.collect.ImmutableSet;
import io.crate.action.FutureActionListener;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.PermissionDeniedException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.metadata.UsersMetaData;
import io.crate.metadata.UsersPrivilegesMetaData;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public class UserManagerService implements UserManager, ClusterStateListener {

    public static User CRATE_USER = new User("crate", EnumSet.of(User.Role.SUPERUSER), ImmutableSet.of());

    private static final PermissionVisitor PERMISSION_VISITOR = new PermissionVisitor();

    static {
        MetaData.registerPrototype(UsersMetaData.TYPE, UsersMetaData.PROTO);
        MetaData.registerPrototype(UsersPrivilegesMetaData.TYPE, UsersPrivilegesMetaData.PROTO);
    }

    private final TransportCreateUserAction transportCreateUserAction;
    private final TransportDropUserAction transportDropUserAction;
    private final TransportPrivilegesAction transportPrivilegesAction;
    private volatile Set<User> users = ImmutableSet.of(CRATE_USER);

    public UserManagerService(TransportCreateUserAction transportCreateUserAction,
                              TransportDropUserAction transportDropUserAction,
                              TransportPrivilegesAction transportPrivilegesAction,
                              ClusterService clusterService) {
        this.transportCreateUserAction = transportCreateUserAction;
        this.transportDropUserAction = transportDropUserAction;
        this.transportPrivilegesAction = transportPrivilegesAction;
        clusterService.add(this);
    }

    static Set<User> getUsers(@Nullable UsersMetaData metaData,
                              @Nullable UsersPrivilegesMetaData privilegesMetaData) {
        ImmutableSet.Builder<User> usersBuilder = new ImmutableSet.Builder<User>().add(CRATE_USER);
        if (metaData != null) {
            for (String userName : metaData.users()) {
                Set<Privilege> privileges = null;
                if (privilegesMetaData != null) {
                    privileges = privilegesMetaData.getUserPrivileges(userName);
                }
                usersBuilder.add(new User(userName, ImmutableSet.of(), privileges == null ? ImmutableSet.of() : privileges));
            }
        }
        return usersBuilder.build();
    }

    @Override
    public CompletableFuture<Long> createUser(String userName) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        transportCreateUserAction.execute(new CreateUserRequest(userName), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> dropUser(String userName, boolean ifExists) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(WriteUserResponse::affectedRows);
        transportDropUserAction.execute(new DropUserRequest(userName, ifExists), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> userNames, Collection<Privilege> privileges) {
        FutureActionListener<PrivilegesResponse, Long> listener = new FutureActionListener<>(PrivilegesResponse::affectedRows);
        transportPrivilegesAction.execute(new PrivilegesRequest(userNames, privileges), listener);
        return listener;
    }

    public Iterable<User> users() {
        return users;
    }

    @Override
    public void ensureAuthorized(AnalyzedStatement analyzedStatement,
                                 SessionContext sessionContext) {
        PERMISSION_VISITOR.process(analyzedStatement, sessionContext);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        MetaData metaData = event.state().metaData();
        users = getUsers(metaData.custom(UsersMetaData.TYPE), metaData.custom(UsersPrivilegesMetaData.TYPE));
    }


    @Nullable
    public User findUser(String userName) {
        for (User user: users()) {
            if (userName.equals(user.name())) {
                return user;
            }
        }
        return null;
    }

    @Override
    public void raiseMissingPrivilegeException(Privilege.Clazz clazz, @Nullable Privilege.Type type, String ident, User user) throws PermissionDeniedException {
        if (null == type) {
            return;
        }
        assert user != null : "the user must never be null";

        if (!user.isSuperUser() && Privilege.Type.DCL.equals(type)) {
            throw new PermissionDeniedException(user.name(), type);
        }

        if (user.isSuperUser()) {
            return;
        } else if (!user.hasPrivilege(type, clazz, ident)) {
            throw new PermissionDeniedException(user.name(), type);
        }
    }

    private static class PermissionVisitor extends AnalyzedStatementVisitor<SessionContext, Boolean> {

        boolean isSuperUser(@Nullable User user) {
            return user != null && user.roles().contains(User.Role.SUPERUSER);
        }

        private void throwUnauthorized(@Nullable User user) {
            String userName = user != null ? user.name() : null;
            throw new UnauthorizedException(
                String.format(Locale.ENGLISH, "User \"%s\" is not authorized to execute statement", userName));
        }

        @Override
        protected Boolean visitAnalyzedStatement(AnalyzedStatement analyzedStatement,
                                                 SessionContext sessionContext) {
            return true;
        }

        @Override
        protected Boolean visitCreateUserStatement(CreateUserAnalyzedStatement analysis,
                                                   SessionContext sessionContext) {
            if (!isSuperUser(sessionContext.user())) {
                throwUnauthorized(sessionContext.user());
            }
            return true;
        }

        @Override
        protected Boolean visitDropUserStatement(DropUserAnalyzedStatement analysis,
                                                 SessionContext sessionContext) {
            if (!isSuperUser(sessionContext.user())) {
                throwUnauthorized(sessionContext.user());
            }
            return true;
        }
    }
}

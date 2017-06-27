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
import io.crate.exceptions.UnauthorizedException;
import io.crate.exceptions.UserAlreadyExistsException;
import io.crate.exceptions.UserUnknownException;
import io.crate.metadata.UsersMetaData;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static io.crate.metadata.UsersMetaData.PROTO;
import static io.crate.metadata.UsersMetaData.TYPE;

public class UserManagerService implements UserManager, ClusterStateListener {

    public static User CRATE_USER = new User("crate", EnumSet.of(User.Role.SUPERUSER));

    private static final PermissionVisitor PERMISSION_VISITOR = new PermissionVisitor();

    private static final Consumer<User> ENSURE_DROP_USER_NOT_SUPERUSER = user -> {
        if (user != null && user.isSuperUser()) {
            throw new UnsupportedOperationException(String.format(
                Locale.ENGLISH, "Cannot drop a superuser '%s'", user.name()));
        }
    };

    static {
        MetaData.registerPrototype(TYPE, PROTO);
    }

    private final TransportCreateUserAction transportCreateUserAction;
    private final TransportDropUserAction transportDropUserAction;
    private volatile Set<User> users = ImmutableSet.of(CRATE_USER);

    public UserManagerService(TransportCreateUserAction transportCreateUserAction,
                              TransportDropUserAction transportDropUserAction,
                              ClusterService clusterService) {
        this.transportCreateUserAction = transportCreateUserAction;
        this.transportDropUserAction = transportDropUserAction;
        clusterService.add(this);
    }

    static Set<User> getUsers(@Nullable UsersMetaData metaData) {
        ImmutableSet.Builder<User> usersBuilder = new ImmutableSet.Builder<User>().add(CRATE_USER);
        if (metaData != null) {
            for (String userName : metaData.users()) {
                usersBuilder.add(new User(userName, ImmutableSet.of()));
            }
        }
        return usersBuilder.build();
    }

    @Override
    public CompletableFuture<Long> createUser(String userName) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> {
            if (r.doesUserExist()) {
                throw new UserAlreadyExistsException(userName);
            }
            return 1L;
        });
        transportCreateUserAction.execute(new CreateUserRequest(userName), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> dropUser(String userName, boolean ifExists) {
        ENSURE_DROP_USER_NOT_SUPERUSER.accept(findUser(userName));
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> {
            if (r.doesUserExist() == false) {
                if (ifExists) {
                    return 0L;
                }
                throw new UserUnknownException(userName);
            }
            return 1L;
        });
        transportDropUserAction.execute(new DropUserRequest(userName, ifExists), listener);
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
        users = getUsers(event.state().metaData().custom(UsersMetaData.TYPE));
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

    private static class PermissionVisitor extends AnalyzedStatementVisitor<SessionContext, Boolean> {

        boolean isSuperUser(@Nullable User user) {
            return user != null && user.isSuperUser();
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

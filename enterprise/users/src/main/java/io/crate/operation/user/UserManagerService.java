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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.crate.action.FutureActionListener;
import io.crate.analyze.user.Privilege;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.exceptions.UserAlreadyExistsException;
import io.crate.exceptions.UserUnknownException;
import io.crate.metadata.UsersMetaData;
import io.crate.metadata.UsersPrivilegesMetaData;
import io.crate.metadata.cluster.DDLClusterStateService;
import io.crate.metadata.sys.SysPrivilegesTableInfo;
import io.crate.metadata.sys.SysUsersTableInfo;
import io.crate.operation.collect.sources.SysTableRegistry;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

@Singleton
public class UserManagerService implements UserManager, ClusterStateListener {

    public final static User CRATE_USER = new User("crate", EnumSet.of(User.Role.SUPERUSER), ImmutableSet.of());

    @VisibleForTesting
    static final StatementAuthorizedValidator NOOP_STATEMENT_VALIDATOR = s -> {
    };
    @VisibleForTesting
    static final ExceptionAuthorizedValidator NOOP_EXCEPTION_VALIDATOR = t -> {
    };

    static final StatementAuthorizedValidator ALWAYS_FAIL_STATEMENT_VALIDATOR = s -> {
        throw new UnauthorizedException("User `null` is not authorized to execute statement");
    };

    static final ExceptionAuthorizedValidator ALWAYS_FAIL_EXCEPTION_VALIDATOR = s -> {
        throw new MissingPrivilegeException(s.getMessage());
    };

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
    private final TransportPrivilegesAction transportPrivilegesAction;
    private final TransportTransferTablePrivilegesAction transportTransferTablePrivilegesAction;
    private volatile Set<User> users = ImmutableSet.of(CRATE_USER);

    @Inject
    public UserManagerService(TransportCreateUserAction transportCreateUserAction,
                              TransportDropUserAction transportDropUserAction,
                              TransportPrivilegesAction transportPrivilegesAction,
                              TransportTransferTablePrivilegesAction transportTransferTablePrivilegesAction,
                              SysTableRegistry sysTableRegistry,
                              ClusterService clusterService,
                              DDLClusterStateService ddlClusterStateService) {
        this.transportCreateUserAction = transportCreateUserAction;
        this.transportDropUserAction = transportDropUserAction;
        this.transportPrivilegesAction = transportPrivilegesAction;
        this.transportTransferTablePrivilegesAction = transportTransferTablePrivilegesAction;
        clusterService.addListener(this);
        sysTableRegistry.registerSysTable(new SysUsersTableInfo(clusterService),
            () -> CompletableFuture.completedFuture(users()),
            SysUsersTableInfo.sysUsersExpressions());

        sysTableRegistry.registerSysTable(new SysPrivilegesTableInfo(clusterService),
            () -> CompletableFuture.completedFuture(SysPrivilegesTableInfo.buildPrivilegesRows(users())),
            SysPrivilegesTableInfo.expressions());

        ddlClusterStateService.addModifier(DDL_MODIFIER);
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
                usersBuilder.add(new User(userName, ImmutableSet.of(),
                    privileges == null ? ImmutableSet.of() : privileges));
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
            //noinspection PointlessBooleanExpression
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

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> userNames, Collection<Privilege> privileges) {
        userNames.forEach(s -> ENSURE_PRIVILEGE_USER_NOT_SUPERUSER.accept(findUser(s)));
        FutureActionListener<ApplyPrivilegesResponse, Long> listener = new FutureActionListener<>(r -> {
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
    public CompletableFuture<Long> transferTablePrivileges(String sourceIdent, String targetIdent) {
        FutureActionListener<PrivilegesResponse, Long> listener =
            new FutureActionListener<>(PrivilegesResponse::affectedRows);
        transportTransferTablePrivilegesAction.execute(new TransferTablePrivilegesRequest(sourceIdent, targetIdent), listener);
        return listener;
    }

    public Iterable<User> users() {
        return users;
    }

    @Override
    public StatementAuthorizedValidator getStatementValidator(@Nullable User user) {
        if (user == null) {
            return ALWAYS_FAIL_STATEMENT_VALIDATOR;
        }
        if (authorizedValidationRequired(user)) {
            return new StatementPrivilegeValidator(user);
        }
        return NOOP_STATEMENT_VALIDATOR;
    }

    @Override
    public ExceptionAuthorizedValidator getExceptionValidator(@Nullable User user) {
        if (user == null) {
            return ALWAYS_FAIL_EXCEPTION_VALIDATOR;
        }
        if (authorizedValidationRequired(user)) {
            return new ExceptionPrivilegeValidator(user);
        }
        return NOOP_EXCEPTION_VALIDATOR;
    }

    @SuppressWarnings({"SimplifiableIfStatement", "PointlessBooleanExpression"})
    private boolean authorizedValidationRequired(@Nullable User user) {
        return user.isSuperUser() == false;
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
        for (User user : users()) {
            if (userName.equals(user.name())) {
                return user;
            }
        }
        return null;
    }
}

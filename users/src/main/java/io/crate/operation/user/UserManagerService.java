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
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.internal.Nullable;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.crate.operation.user.UsersMetaData.PROTO;
import static io.crate.operation.user.UsersMetaData.TYPE;

public class UserManagerService implements UserManager, ClusterStateListener {

    static User CRATE_USER = new User("crate", true);
    private volatile Set<User> users = ImmutableSet.of(CRATE_USER);

    static {
        MetaData.registerPrototype(TYPE, PROTO);
    }

    private final TransportCreateUserAction transportCreateUserAction;
    private final TransportDropUserAction transportDropUserAction;

    UserManagerService(TransportCreateUserAction transportCreateUserAction,
                       TransportDropUserAction transportDropUserAction,
                       ClusterService clusterService) {
        this.transportCreateUserAction = transportCreateUserAction;
        this.transportDropUserAction = transportDropUserAction;
        clusterService.add(this);
    }

    @Override
    public CompletableFuture<Long> createUser(CreateUserAnalyzedStatement analysis) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        transportCreateUserAction.execute(new CreateUserRequest(analysis.userName()), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> dropUser(DropUserAnalyzedStatement analysis) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        transportDropUserAction.execute(new DropUserRequest(analysis.userName()), listener);
        return listener;
    }

    public Iterable<User> userGetter() {
        return users;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        users = getUsersFromMetaData(event.state().metaData().custom(UsersMetaData.TYPE));
    }

    static Set<User> getUsersFromMetaData(@Nullable UsersMetaData metaData) {
        ImmutableSet.Builder<User> usersBuilder = new ImmutableSet.Builder<User>().add(CRATE_USER);
        if (metaData != null) {
            for (String userName : metaData.users()) {
                usersBuilder.add(new User(userName, false));
            }
        }
        return usersBuilder.build();
    }
}

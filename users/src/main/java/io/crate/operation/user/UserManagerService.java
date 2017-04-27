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

import io.crate.action.FutureActionListener;
import io.crate.analyze.CreateUserAnalyzedStatement;
import io.crate.analyze.DropUserAnalyzedStatement;
import io.crate.concurrent.CompletableFutures;
import org.elasticsearch.cluster.metadata.MetaData;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.concurrent.CompletableFuture;

import static io.crate.operation.user.UsersMetaData.PROTO;
import static io.crate.operation.user.UsersMetaData.TYPE;

public class UserManagerService implements UserManager {

    static {
        MetaData.registerPrototype(TYPE, PROTO);
    }

    private final TransportCreateUserAction transportCreateUserAction;

    UserManagerService(TransportCreateUserAction transportCreateUserAction) {
        this.transportCreateUserAction = transportCreateUserAction;
    }

    @Override
    public CompletableFuture<Long> createUser(CreateUserAnalyzedStatement analysis) {
        FutureActionListener<WriteUserResponse, Long> listener = new FutureActionListener<>(r -> 1L);
        transportCreateUserAction.execute(new CreateUserRequest(analysis.userName()), listener);
        return listener;
    }

    @Override
    public CompletableFuture<Long> dropUser(DropUserAnalyzedStatement analysis) {
        return CompletableFutures.failedFuture(new NotImplementedException());
    }
}

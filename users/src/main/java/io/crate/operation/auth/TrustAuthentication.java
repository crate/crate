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

package io.crate.operation.auth;

import io.crate.operation.user.User;
import io.crate.protocols.postgres.Messages;
import org.jboss.netty.channel.Channel;

import java.util.EnumSet;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;


public class TrustAuthentication implements AuthenticationMethod {

    static final String NAME = "trust";
    private static final User SUPERUSER = new User("crate", EnumSet.of(User.Role.SUPERUSER));

    @Override
    public CompletableFuture<User> pgAuthenticate(Channel channel, String userName) {
        CompletableFuture<User> future = new CompletableFuture<>();
        if (SUPERUSER.name().equals(userName)) {
            Messages.sendAuthenticationOK(channel)
                .addListener(f -> future.complete(SUPERUSER));
        } else {
            future.completeExceptionally(
                new Throwable(String.format(Locale.ENGLISH, "trust authentication failed for user \"%s\"", userName))
            );
        }
        return future;
    }

    @Override
    public String name() {
        return NAME;
    }

}

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

import io.crate.action.sql.SessionContext;
import io.crate.operation.user.UserManager;
import io.crate.protocols.postgres.Messages;
import org.jboss.netty.channel.Channel;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;


public class TrustAuthentication implements AuthenticationMethod {

    static final String NAME = "trust";
    private final UserManager userManager;

    public TrustAuthentication(UserManager userManager) {
        this.userManager = userManager;
    }

    @Override
    public CompletableFuture<Boolean> pgAuthenticate(Channel channel, SessionContext session) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        if (userManager.userExists(session.userName())) {
            Messages.sendAuthenticationOK(channel)
                .addListener(f -> future.complete(true));
        } else {
            Messages.sendAuthenticationError(
                channel,
                String.format(Locale.ENGLISH, "trust authentication failed for user \"%s\"", session.userName())
            ).addListener(f -> future.complete(false));
        }
        return future;
    }

    @Override
    public String name() {
        return NAME;
    }

}

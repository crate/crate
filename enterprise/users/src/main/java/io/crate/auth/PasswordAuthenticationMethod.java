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

package io.crate.auth;

import io.crate.operation.user.User;
import io.crate.operation.user.UserLookup;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.user.SecureHash;
import org.elasticsearch.common.settings.SecureString;

import javax.annotation.Nullable;

public class PasswordAuthenticationMethod implements AuthenticationMethod {

    public static final String NAME = "password";
    private UserLookup userLookup;

    PasswordAuthenticationMethod(UserLookup userLookup) {
        this.userLookup = userLookup;
    }

    @Nullable
    @Override
    public User authenticate(String userName, SecureString passwd, ConnectionProperties connProperties) {
        User user = userLookup.findUser(userName);
        if (user != null && passwd != null && passwd.length() > 0) {
            SecureHash secureHash = user.password();
            if (secureHash != null && secureHash.verifyHash(passwd)) {
                return user;
            }
        }
        throw new RuntimeException("password authentication failed for user \"" + userName + "\"");
    }

    @Override
    public String name() {
        return NAME;
    }
}

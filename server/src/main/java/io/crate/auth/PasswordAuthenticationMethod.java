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

package io.crate.auth;

import io.crate.user.User;
import io.crate.user.UserLookup;
import io.crate.protocols.postgres.ConnectionProperties;
import io.crate.user.SecureHash;
import org.elasticsearch.common.settings.SecureString;

import javax.annotation.Nullable;

public class PasswordAuthenticationMethod implements AuthenticationMethod {

    public static final String NAME = "password";
    private final UserLookup userLookup;

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

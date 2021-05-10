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

package io.crate.user;

import io.crate.action.sql.SessionContext;
import io.crate.auth.AccessControl;
import io.crate.exceptions.UnsupportedFeatureException;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class StubUserManager implements UserManager {

    @Override
    public CompletableFuture<Long> createUser(String userName, @Nullable SecureHash hashedPw) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("CREATE USER is only supported in enterprise version"));
    }

    @Override
    public CompletableFuture<Long> dropUser(String userName, boolean suppressNotFoundError) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("DROP USER is only supported in enterprise version"));
    }

    @Override
    public CompletableFuture<Long> alterUser(String userName, @Nullable SecureHash newHashedPw) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("ALTER USER is only supported in enterprise version"));
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> userNames, Collection<Privilege> privileges) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("GRANT or REVOKE privileges is only supported in enterprise version"));
    }

    @Nullable
    @Override
    public User findUser(String userName) {
        // Without enterprise enabled everything runs as superuser
        return User.CRATE_USER;
    }

    @Override
    public AccessControl getAccessControl(SessionContext sessionContext) {
        return AccessControl.DISABLED;
    }
}

/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.user;

import io.crate.analyze.user.Privilege;
import io.crate.concurrent.CompletableFutures;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.operation.user.ExceptionAuthorizedValidator;
import io.crate.operation.user.StatementAuthorizedValidator;
import io.crate.operation.user.User;
import io.crate.operation.user.UserManager;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class StubUserManager implements UserManager {

    @Override
    public CompletableFuture<Long> createUser(String userName) {
        return CompletableFutures.failedFuture(
            new UnsupportedFeatureException("CREATE USER is only supported in enterprise version")
        );
    }

    @Override
    public CompletableFuture<Long> dropUser(String userName, boolean ifExists) {
        return CompletableFutures.failedFuture(
            new UnsupportedFeatureException("DROP USER is only supported in enterprise version")
        );
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> userNames, Collection<Privilege> privileges) {
        return CompletableFutures.failedFuture(
            new UnsupportedFeatureException("GRANT or REVOKE privileges is only supported in enterprise version")
        );
    }

    @Override
    public CompletableFuture<Long> transferTablePrivileges(String sourceIdent, String targetIdent) {
        return CompletableFuture.completedFuture(-1L);
    }

    @Nullable
    @Override
    public User findUser(String userName) {
        return null;
    }

    @Override
    public StatementAuthorizedValidator getStatementValidator(@Nullable User user) {
        return s -> {
        };
    }

    @Override
    public ExceptionAuthorizedValidator getExceptionValidator(@Nullable User user) {
        return t -> {
        };
    }
}

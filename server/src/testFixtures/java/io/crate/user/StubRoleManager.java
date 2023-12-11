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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.settings.CoordinatorSessionSettings;

public class StubRoleManager implements RoleManager {

    private final List<Role> roles = List.of(Role.CRATE_USER);

    @Override
    public CompletableFuture<Long> createRole(String roleName, boolean isUser, @Nullable SecureHash hashedPw) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("createRole is not implemented in StubRoleManager"));
    }

    @Override
    public CompletableFuture<Long> dropRole(String roleName, boolean suppressNotFoundError) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("dropRole is not implemented in StubRoleManager"));
    }

    @Override
    public CompletableFuture<Long> alterRole(String roleName, @Nullable SecureHash newHashedPw) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("alterRole is not implemented in StubRoleManager"));
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> roleNames, Collection<Privilege> privileges) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("applyPrivileges is not implemented in StubRoleManager"));
    }

    @Override
    public Role findUser(int userOid) {
        return Role.CRATE_USER;
    }

    @Override
    public Role findUser(String userName) {
        return Role.CRATE_USER;
    }

    @Override
    public Collection<Role> roles() {
        return roles;
    }

    @Override
    public AccessControl getAccessControl(CoordinatorSessionSettings sessionSettings) {
        return AccessControl.DISABLED;
    }
}

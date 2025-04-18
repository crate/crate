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

package io.crate.role;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.auth.AccessControlImpl;
import io.crate.exceptions.UnsupportedFeatureException;

public class StubRoleManager implements RoleManager {

    private final Collection<Role> roles;
    private final boolean accessControl;

    public StubRoleManager() {
        this.roles = List.of(Role.CRATE_USER);
        this.accessControl = false;
    }

    public StubRoleManager(Collection<Role> roles, boolean accessControl) {
        this.roles = roles;
        this.accessControl = accessControl;
    }

    @Override
    public CompletableFuture<Long> createRole(String roleName,
                                              boolean isUser,
                                              @Nullable SecureHash hashedPw,
                                              @Nullable JwtProperties jwtProperties) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("createRole is not implemented in StubRoleManager"));
    }

    @Override
    public CompletableFuture<Long> dropRole(String roleName, boolean suppressNotFoundError) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("dropRole is not implemented in StubRoleManager"));
    }

    @Override
    public CompletableFuture<Long> alterRole(String roleName,
                                             @Nullable SecureHash newHashedPw,
                                             @Nullable JwtProperties newJwtProperties,
                                             boolean resetPassword,
                                             boolean resetJwtProperties,
                                             Map<Boolean, Map<String, Object>> sessionSettingsChange) {
        return CompletableFuture.failedFuture(new UnsupportedFeatureException("alterRole is not implemented in StubRoleManager"));
    }

    @Override
    public CompletableFuture<Long> applyPrivileges(Collection<String> roleNames,
                                                   Collection<Privilege> privileges,
                                                   GrantedRolesChange grantedRolesChange) {
        return CompletableFuture.failedFuture(
            new UnsupportedFeatureException("applyPrivileges is not implemented in StubRoleManager"));
    }

    @Override
    public Collection<Role> roles() {
        return roles;
    }

    @Override
    public AccessControl getAccessControl(Role authenticatedUser, Role sessionUser) {
        if (accessControl) {
            return new AccessControlImpl(this, authenticatedUser, sessionUser);
        }
        return AccessControl.DISABLED;
    }
}

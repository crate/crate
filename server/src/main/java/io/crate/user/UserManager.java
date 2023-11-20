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
import java.util.concurrent.CompletableFuture;

import org.jetbrains.annotations.Nullable;

import io.crate.auth.AccessControl;
import io.crate.metadata.settings.CoordinatorSessionSettings;

/**
 * responsible for creating and deleting users
 */
public interface UserManager extends RoleLookup {

    /**
     * Create a role.
     * This never raises but always returns a failed future in error cases.
     *
     * @param roleName name of the role to create
     * @return 1 if the user was created, otherwise a failed future.
     */
    CompletableFuture<Long> createRole(String roleName, boolean isUser, @Nullable SecureHash hashedPw);

    /**
     * Delete a user.
     * This never raises but always returns a failed future in error cases.
     *
     * @param userName the user to drop
     * @return 1 if dropped, 0 if not found and {@code suppressNotFoundError} is true.
     *         Otherwise a failure
     */
    CompletableFuture<Long> dropRole(String userName, boolean suppressNotFoundError);

    /**
     * Modifies a user/role.
     * This never raises but always returns a failed future in error cases.
     *
     * @param roleName of the existing role to modify
     * @param newHashedPw new password; if null the password is removed from the user
     * @return 1 if the user has been updated, otherwise a failed future.
     */
    CompletableFuture<Long> alterRole(String roleName, @Nullable SecureHash newHashedPw);

    /**
     * Apply given list of {@link Privilege}s for each given user

     * @param userNames     List of user names all privileges should be applied for
     * @param privileges    List of privileges to apply
     * @return a future which returns the number of privileges which were successfully applied
     */
    CompletableFuture<Long> applyPrivileges(Collection<String> userNames, Collection<Privilege> privileges);

    AccessControl getAccessControl(CoordinatorSessionSettings sessionSettings);
}

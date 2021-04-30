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

package io.crate.action.sql;

import io.crate.user.User;
import io.crate.metadata.SearchPath;
import io.crate.planner.optimizer.Rule;

import java.util.HashSet;
import java.util.Set;

import static io.crate.metadata.SearchPath.createSearchPathFrom;
import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;
import static java.util.Objects.requireNonNull;

public class SessionContext {

    private final User authenticatedUser;
    private User sessionUser;

    private SearchPath searchPath;
    private boolean hashJoinEnabled = true;
    private Set<Class<? extends Rule<?>>> excludedOptimizerRules;

    /**
     * Creates a new SessionContext suitable to use as system SessionContext
     */
    public static SessionContext systemSessionContext() {
        return new SessionContext(User.CRATE_USER, User.CRATE_USER);
    }

    public SessionContext(User authenticatedUser, String... searchPath) {
        this(authenticatedUser, authenticatedUser, searchPath);
    }

    public SessionContext(User authenticatedUser, User sessionUser, String... searchPath) {
        this.authenticatedUser = requireNonNull(authenticatedUser, "Authenticated user is required");
        this.sessionUser = requireNonNull(sessionUser, "Session user is required");
        this.searchPath = createSearchPathFrom(searchPath);
        this.excludedOptimizerRules = new HashSet<>();
    }

    /**
     * Reverts the schema to the built-in default.
     */
    public void resetSchema() {
        searchPath = pathWithPGCatalogAndDoc();
    }

    public SearchPath searchPath() {
        return searchPath;
    }

    public void setSearchPath(SearchPath searchPath) {
        this.searchPath = searchPath;
    }

    public void setSearchPath(String... schemas) {
        this.searchPath = createSearchPathFrom(schemas);
    }

    public boolean isHashJoinEnabled() {
        return hashJoinEnabled;
    }

    public void setHashJoinEnabled(boolean hashJoinEnabled) {
        this.hashJoinEnabled = hashJoinEnabled;
    }

    public User authenticatedUser() {
        return authenticatedUser;
    }

    public User sessionUser() {
        return sessionUser;
    }

    public void setSessionUser(User user) {
        sessionUser = user;
    }

    public Set<Class<? extends Rule<?>>> excludedOptimizerRules() {
        return excludedOptimizerRules;
    }

    public void resetToDefaults() {
        resetSchema();
        hashJoinEnabled = true;
    }
}

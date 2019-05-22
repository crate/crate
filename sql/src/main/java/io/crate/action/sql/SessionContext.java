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

package io.crate.action.sql;

import io.crate.auth.user.User;
import io.crate.metadata.SearchPath;

import java.util.Set;

import static io.crate.metadata.SearchPath.createSearchPathFrom;
import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;
import static java.util.Objects.requireNonNull;

public class SessionContext {

    private final Set<Option> options;
    private final User user;

    private SearchPath searchPath;
    private boolean hashJoinEnabled = true;

    /**
     * Creates a new SessionContext suitable to use as system SessionContext
     */
    public static SessionContext systemSessionContext() {
        return new SessionContext(Option.NONE, User.CRATE_USER);
    }

    public SessionContext(Set<Option> options, User user, String... searchPath) {
        this.options = options;
        this.user = requireNonNull(user, "User is required");
        this.searchPath = createSearchPathFrom(searchPath);
    }

    /**
     * Reverts the schema to the built-in default.
     */
    public void resetSchema() {
        searchPath = pathWithPGCatalogAndDoc();
    }

    public Set<Option> options() {
        return options;
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

    public User user() {
        return user;
    }

    public void resetToDefaults() {
        resetSchema();
        hashJoinEnabled = true;
    }
}

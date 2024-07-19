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

package io.crate.metadata.settings;

import static io.crate.Constants.DEFAULT_DATE_STYLE;

import java.util.HashSet;
import java.util.Set;

import io.crate.common.unit.TimeValue;
import io.crate.metadata.SearchPath;
import io.crate.planner.optimizer.LoadedRules;
import io.crate.planner.optimizer.Rule;
import io.crate.role.Role;

/**
 * A superset of {@link SessionSettings}.
 * Contains all available session settings including their setters
 *
 * <p>
 * A subset of this information can be streamed as {@link SessionSettings}
 * </p>
 */
public class CoordinatorSessionSettings extends SessionSettings {

    private final Role authenticatedUser;
    private Role sessionUser;
    private Set<Class<? extends Rule<?>>> excludedOptimizerRules;
    private String applicationName;
    private String dateStyle;
    private TimeValue statementTimeout;

    public CoordinatorSessionSettings(Role authenticatedUser, String... searchPath) {
        this(authenticatedUser, authenticatedUser, Set.of(), searchPath);
    }

    public CoordinatorSessionSettings(Role authenticatedUser, Role sessionUser, String... searchPath) {
        this(
            authenticatedUser,
            sessionUser,
            Set.of(),
            searchPath
        );
    }

    public CoordinatorSessionSettings(Role authenticatedUser,
                                      Role sessionUser,
                                      Set<Class<? extends Rule<?>>> excludedOptimizerRules,
                                      String... searchPath) {
        this(
            authenticatedUser,
            sessionUser,
            SearchPath.createSearchPathFrom(searchPath),
            true,
            excludedOptimizerRules,
            true,
            0
        );
    }

    public CoordinatorSessionSettings(Role authenticatedUser,
                                      Role sessionUser,
                                      SearchPath searchPath,
                                      boolean hashJoinsEnabled,
                                      Set<Class<? extends Rule<?>>> excludedOptimizerRules,
                                      boolean errorOnUnknownObjectKey,
                                      int memoryLimit) {
        super(authenticatedUser.name(), searchPath, hashJoinsEnabled, errorOnUnknownObjectKey, memoryLimit);
        this.authenticatedUser = authenticatedUser;
        this.sessionUser = sessionUser;
        this.excludedOptimizerRules = new HashSet<>(excludedOptimizerRules);
        this.dateStyle = DEFAULT_DATE_STYLE;
        this.statementTimeout = TimeValue.ZERO;
        this.memoryLimit = memoryLimit;
    }

    /**
     * Current active user.
     * <p>
     * This is either the same as {@link #authenticatedUser()} or the user set via
     * SET SESSION AUTHORIZATION
     * </p>
     **/
    public Role sessionUser() {
        return sessionUser;
    }

    /**
     * The user as it originally authenticated against CrateDB.
     * In most cases you should use {@link #sessionUser()} instead
     **/
    public Role authenticatedUser() {
        return authenticatedUser;
    }

    public static CoordinatorSessionSettings systemDefaults() {
        return new CoordinatorSessionSettings(Role.CRATE_USER, Role.CRATE_USER, LoadedRules.INSTANCE.disabledRules());
    }

    public void setErrorOnUnknownObjectKey(boolean newValue) {
        errorOnUnknownObjectKey = newValue;
    }

    public void setSearchPath(String ... schemas) {
        this.searchPath = SearchPath.createSearchPathFrom(schemas);
    }

    public void setSearchPath(SearchPath searchPath) {
        this.searchPath = searchPath;
    }

    public void setHashJoinEnabled(boolean newValue) {
        hashJoinsEnabled = newValue;
    }

    public void setSessionUser(Role user) {
        sessionUser = user;
        userName = user.name();
    }

    public Set<Class<? extends Rule<?>>> excludedOptimizerRules() {
        return excludedOptimizerRules;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    @Override
    public String applicationName() {
        return this.applicationName;
    }

    public void setDateStyle(String dateStyle) {
        this.dateStyle = dateStyle;
    }

    @Override
    public String dateStyle() {
        return this.dateStyle;
    }

    public TimeValue statementTimeout() {
        return statementTimeout;
    }

    public void statementTimeout(TimeValue statementTimeout) {
        this.statementTimeout = statementTimeout;
    }

    public void memoryLimit(int memoryLimit) {
        this.memoryLimit = memoryLimit;
    }
}

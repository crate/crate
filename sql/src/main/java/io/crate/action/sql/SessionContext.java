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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.AnalyzedStatement;
import io.crate.auth.user.ExceptionAuthorizedValidator;
import io.crate.auth.user.StatementAuthorizedValidator;
import io.crate.auth.user.User;
import io.crate.exceptions.MissingPrivilegeException;

import java.util.Set;

import static io.crate.metadata.SearchPath.createSearchPathFrom;
import static io.crate.metadata.SearchPath.pathWithPGCatalogAndDoc;
import static java.util.Objects.requireNonNull;

public class SessionContext implements StatementAuthorizedValidator, ExceptionAuthorizedValidator {

    private final int defaultLimit;
    private final Set<Option> options;
    private final User user;
    private final StatementAuthorizedValidator statementAuthorizedValidator;
    private final ExceptionAuthorizedValidator exceptionAuthorizedValidator;

    private SearchPath searchPath;
    private boolean semiJoinsRewriteEnabled = false;
    private boolean hashJoinEnabled = true;

    /**
     * Creates a new SessionContext suitable to use as system SessionContext
     */
    public static SessionContext systemSessionContext() {
        return new SessionContext(0, Option.NONE, User.CRATE_USER, s -> { }, e -> { });
    }

    public SessionContext(User user,
                          StatementAuthorizedValidator statementAuthorizedValidator,
                          ExceptionAuthorizedValidator exceptionAuthorizedValidator,
                          String... searchPath) {
        this(0, Option.NONE, user,
            statementAuthorizedValidator, exceptionAuthorizedValidator, searchPath);

    }

    public SessionContext(int defaultLimit,
                          Set<Option> options,
                          User user,
                          StatementAuthorizedValidator statementAuthorizedValidator,
                          ExceptionAuthorizedValidator exceptionAuthorizedValidator,
                          String... searchPath) {
        this.defaultLimit = defaultLimit;
        this.options = options;
        this.user = requireNonNull(user, "User is required");
        this.statementAuthorizedValidator = statementAuthorizedValidator;
        this.exceptionAuthorizedValidator = exceptionAuthorizedValidator;
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

    public String defaultSchema() {
        return searchPath.defaultSchema();
    }

    public String currentSchema() {
        return searchPath.currentSchema();
    }

    public void setSearchPath(String... schemas) {
        this.searchPath = createSearchPathFrom(schemas);
    }

    public void setSemiJoinsRewriteEnabled(boolean flag) {
        this.semiJoinsRewriteEnabled = flag;
    }

    public boolean getSemiJoinsRewriteEnabled() {
        return semiJoinsRewriteEnabled;
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

    public int defaultLimit() {
        return defaultLimit;
    }

    @Override
    public void ensureExceptionAuthorized(Throwable t) throws MissingPrivilegeException {
        exceptionAuthorizedValidator.ensureExceptionAuthorized(t);
    }

    @Override
    public void ensureStatementAuthorized(AnalyzedStatement statement) {
        statementAuthorizedValidator.ensureStatementAuthorized(statement);
    }

    public void resetToDefaults() {
        resetSchema();
        semiJoinsRewriteEnabled = false;
        hashJoinEnabled = true;
    }
}

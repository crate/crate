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

import io.crate.analyze.AnalyzedStatement;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.metadata.Schemas;
import io.crate.operation.user.ExceptionAuthorizedValidator;
import io.crate.operation.user.StatementAuthorizedValidator;
import io.crate.operation.user.User;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public class SessionContext implements StatementAuthorizedValidator, ExceptionAuthorizedValidator {

    private final int defaultLimit;
    private final Set<Option> options;
    @Nullable
    private final User user;
    private final StatementAuthorizedValidator statementAuthorizedValidator;
    private final ExceptionAuthorizedValidator exceptionAuthorizedValidator;

    private String defaultSchema;
    private boolean semiJoinsRewriteEnabled;

    public SessionContext(@Nullable String defaultSchema,
                          @Nullable User user,
                          StatementAuthorizedValidator statementAuthorizedValidator,
                          ExceptionAuthorizedValidator exceptionAuthorizedValidator) {
        this(0, Option.NONE, defaultSchema, user,
            statementAuthorizedValidator, exceptionAuthorizedValidator);
    }

    public SessionContext(int defaultLimit,
                          Set<Option> options,
                          @Nullable String defaultSchema,
                          @Nullable User user,
                          StatementAuthorizedValidator statementAuthorizedValidator,
                          ExceptionAuthorizedValidator exceptionAuthorizedValidator) {
        this.defaultLimit = defaultLimit;
        this.options = options;
        this.user = user;
        this.statementAuthorizedValidator = statementAuthorizedValidator;
        this.exceptionAuthorizedValidator = exceptionAuthorizedValidator;
        this.defaultSchema = defaultSchema;
        if (defaultSchema == null) {
            resetSchema();
        }
    }

    /**
     * Reverts the schema to the built-in default.
     */
    public void resetSchema() {
        defaultSchema = Schemas.DEFAULT_SCHEMA_NAME;
    }

    public Set<Option> options() {
        return options;
    }

    public String defaultSchema() {
        return defaultSchema;
    }

    public void setDefaultSchema(String schema) {
        defaultSchema = Objects.requireNonNull(schema, "Default schema must never be set to null");
    }

    public void setSemiJoinsRewriteEnabled(boolean flag) {
        this.semiJoinsRewriteEnabled = flag;
    }

    public boolean getSemiJoinsRewriteEnabled() {
        return semiJoinsRewriteEnabled;
    }

    @Nullable
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

    /**
     * Creates a new SessionContext with default settings.
     */
    public static SessionContext create() {
        return new SessionContext(null, null, s -> {}, t -> {});
    }
}

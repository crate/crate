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

package io.crate.protocols;

import static io.crate.protocols.postgres.PGErrorStatus.AMBIGUOUS_ALIAS;
import static io.crate.protocols.postgres.PGErrorStatus.AMBIGUOUS_COLUMN;
import static io.crate.protocols.postgres.PGErrorStatus.INVALID_SCHEMA_NAME;
import static io.crate.protocols.postgres.PGErrorStatus.UNDEFINED_FUNCTION;
import static io.crate.testing.TestingHelpers.createReference;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.List;

import org.hamcrest.Matcher;
import org.junit.Test;

import io.crate.auth.AccessControl;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.protocols.postgres.PGError;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.rest.action.HttpError;
import io.crate.types.DataTypes;
import io.netty.handler.codec.http.HttpResponseStatus;

public class ErrorMappingTest {

    @Test
    public void test_ambiguous_column_exception_error_mapping() {
        isError(new AmbiguousColumnException(ColumnIdent.of("x"), createReference("x", DataTypes.STRING)),
                is("Column \"x\" is ambiguous"),
                AMBIGUOUS_COLUMN,
                BAD_REQUEST,
                4006);
    }

    @Test
    public void test_invalid_schema_name_exception_error_mapping() {
        isError(new InvalidSchemaNameException("invalid"),
                is("schema name \"invalid\" is invalid."),
                INVALID_SCHEMA_NAME,
                BAD_REQUEST,
                4002);
    }

    @Test
    public void test_ambiguous_column_alias_exception_error_mapping() {
        isError(new AmbiguousColumnAliasException("x", List.of(createReference("x", DataTypes.STRING))),
                is("Column alias \"x\" is ambiguous"),
                AMBIGUOUS_ALIAS,
                BAD_REQUEST,
                4006);
    }

    @Test
    public void test_user_defined_function_unknown_exception_error_mapping() {
        isError(new UserDefinedFunctionUnknownException("schema", "foo", List.of(DataTypes.STRING)),
                is("Cannot resolve user defined function: 'schema.foo(text)'"),
                UNDEFINED_FUNCTION,
                NOT_FOUND,
                4049);
    }

    private void isError(Throwable t,
                         Matcher<String> msg,
                         PGErrorStatus pgErrorStatus,
                         HttpResponseStatus httpResponseStatus,
                         int errorCode) {
        var throwable = SQLExceptions.prepareForClientTransmission(AccessControl.DISABLED, t);

        var pgError = PGError.fromThrowable(throwable);
        assertThat(pgError.status()).isEqualTo(pgErrorStatus);
        assertThat(pgError.message(), msg);

        var httpError = HttpError.fromThrowable(throwable);
        assertThat(httpError.errorCode()).isEqualTo(errorCode);
        assertThat(httpError.httpResponseStatus()).isEqualTo(httpResponseStatus);
        assertThat(httpError.message(), msg);
    }
}

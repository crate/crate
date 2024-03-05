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

package io.crate.testing;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractThrowableAssert;
import org.elasticsearch.transport.RemoteTransportException;
import org.postgresql.util.PSQLException;
import org.postgresql.util.ServerErrorMessage;

import io.crate.exceptions.SQLExceptions;
import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.rest.action.HttpError;
import io.netty.handler.codec.http.HttpResponseStatus;

public final class SQLErrorAssert extends AbstractThrowableAssert<SQLErrorAssert, Throwable> {

    public SQLErrorAssert(Throwable actual) {
        super(actual, SQLErrorAssert.class);
        hasBeenThrown();
    }

    public SQLErrorAssert hasPGError(PGErrorStatus expectedError) {
        if (actual instanceof PSQLException e) {
            ServerErrorMessage serverErrorMessage = e.getServerErrorMessage();
            String sqlState = serverErrorMessage.getSQLState();
            for (var status : PGErrorStatus.values()) {
                if (status.code().equals(sqlState)) {
                    assertThat(status).isEqualTo(expectedError);
                }
            }
            failure("Expected to find error code for: " + sqlState);
        }
        return this;
    }

    public SQLErrorAssert hasHTTPError(HttpResponseStatus httpResponseCode, int errorCode) {
        if (actual instanceof PSQLException) {
            return this;
        }
        HttpError httpError = HttpError.fromThrowable(actual);
        assertThat(httpError.errorCode())
            .as("Expected an exception with errorCode "
                + errorCode
                + " but "
                + actual.getClass().getSimpleName()
                + " has a different code."
                + " Actual error message: " + SQLExceptions.messageOf(actual)
            )
            .isEqualTo(errorCode);
        assertThat(httpError.httpResponseStatus()).isEqualTo(httpResponseCode);
        return this;
    }

    public SQLErrorAssert hasMessageContaining(String msg) {
        var actualErrorMsg = actual.getMessage();
        if (actual instanceof RemoteTransportException) {
            actualErrorMsg = actual.getCause().getMessage();
        }
        assertThat(actualErrorMsg).contains(msg);
        return this;
    }
}

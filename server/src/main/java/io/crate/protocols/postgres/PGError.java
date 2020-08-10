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

package io.crate.protocols.postgres;

import io.crate.exceptions.SQLExceptions;
import org.postgresql.util.PSQLException;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;


public class PGError {

    public static final byte[] SEVERITY_FATAL = "FATAL".getBytes(StandardCharsets.UTF_8);
    public static final byte[] SEVERITY_ERROR = "ERROR".getBytes(StandardCharsets.UTF_8);

    private final PGErrorStatus status;
    private final String message;

    @Nullable
    private final Throwable throwable;

    public PGError(PGErrorStatus status, String message, @Nullable Throwable throwable) {
        this.status = status;
        this.message = message;
        this.throwable = throwable;

    }

    public PGErrorStatus status() {
        return status;
    }

    @Nullable
    public Throwable throwable() {
        return throwable;
    }

    public String message() {
        return message;
    }

    @Override
    public String toString() {
        return "PGError{" +
               "status=" + status +
               ", message='" + message + '\'' +
               ", throwable=" + throwable +
               '}';
    }

    public static PGError fromThrowable(Throwable throwable) {
        if (throwable instanceof PSQLException) {
            return fromPSQLException((PSQLException) throwable);
        }
        Throwable unwrappedError = SQLExceptions.handleException(throwable, null);
        return new PGError(PGErrorStatus.INTERNAL_ERROR, SQLExceptions.messageOf(throwable), unwrappedError);
    }

    public static PGError fromPSQLException(PSQLException e) {
        var sqlState = e.getServerErrorMessage().getSQLState();
        PGErrorStatus errorStatus = null;
        for (var status :PGErrorStatus.values()) {
            if (status.code().equals(sqlState)) {
                errorStatus = status;
                break;
            }
        }
        assert errorStatus != null : "Unknown psql error code: " + sqlState;
        return new PGError(errorStatus, e.getMessage(), e);
    }
}

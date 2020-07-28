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

import io.crate.auth.user.AccessControl;
import io.crate.exceptions.SQLExceptions;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.common.ParsingException;
import org.postgresql.util.PSQLException;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;

import static io.crate.exceptions.SQLExceptions.isDocumentAlreadyExistsException;

public class PGError  {

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

    public static PGError fromThrowable(Throwable throwable, @Nullable AccessControl accessControl) {

        if (throwable instanceof PSQLException) {
            return fromPSQLException((PSQLException) throwable.getCause());
        }

        Throwable unwrappedError = SQLExceptions.unwrapException(throwable, null);
        //TODO make sure values are masked using accessControl
        PGErrorStatus status;
        String message = null;
        if (throwable instanceof ParsingException) {
            status = PGErrorStatus.INTERNAL_ERROR;
            message = throwable.getMessage();
        } else if (throwable instanceof IllegalArgumentException) {
            status = PGErrorStatus.INTERNAL_ERROR;
            message = throwable.getMessage();
        } else if (unwrappedError instanceof UnsupportedOperationException) {
            status = PGErrorStatus.FEATURE_NOT_SUPPORTED;
        } else if (unwrappedError instanceof ResourceAlreadyExistsException) {
            var resourceAlreadyExistsException = (ResourceAlreadyExistsException) unwrappedError;
            status = PGErrorStatus.DUPLICATE_TABLE;
            message = String.format("Relation '%s' already exists.", resourceAlreadyExistsException.getIndex().getName());
        } else if (isDocumentAlreadyExistsException(unwrappedError)) {
            status = PGErrorStatus.DUPLICATE_OBJECT;
            message = "A document with the same primary key exists already";
        } else {
            status = PGErrorStatus.INTERNAL_ERROR;
            message = SQLExceptions.messageOf(unwrappedError);
        }
        return new PGError(status, message, unwrappedError);
    }

    public static PGError fromPSQLException(PSQLException e) {
        var sqlState = e.getServerErrorMessage().getSQLState();
        PGErrorStatus errorStatus = null;
        for (var status :PGErrorStatus.values()) {
            if (status.code().equals(sqlState)) {
                errorStatus =  status;
                break;
            }
        }
        assert errorStatus != null : "Unknown psql error code: " + sqlState;
        return new PGError(errorStatus, e.getMessage().replace("ERROR: ", ""), e);
    }
}

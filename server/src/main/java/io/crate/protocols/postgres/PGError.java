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

package io.crate.protocols.postgres;

import java.nio.charset.StandardCharsets;

import org.elasticsearch.ElasticsearchException;
import org.jetbrains.annotations.Nullable;

import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.DuplicateKeyException;
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;


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
        var status = PGErrorStatus.INTERNAL_ERROR;
        if (throwable instanceof IllegalArgumentException ||
            throwable instanceof UnsupportedOperationException ||
            throwable instanceof UnsupportedFunctionException) {
            status = PGErrorStatus.FEATURE_NOT_SUPPORTED;
        } else if (throwable instanceof DuplicateKeyException) {
            status = PGErrorStatus.UNIQUE_VIOLATION;
        } else if (throwable instanceof RelationUnknown) {
            status = PGErrorStatus.UNDEFINED_TABLE;
        } else if (throwable instanceof ColumnUnknownException) {
            status = PGErrorStatus.UNDEFINED_COLUMN;
        } else if (throwable instanceof RelationAlreadyExists) {
            status = PGErrorStatus.DUPLICATE_TABLE;
        } else if (throwable instanceof AmbiguousColumnException) {
            status = PGErrorStatus.AMBIGUOUS_COLUMN;
        } else if (throwable instanceof InvalidSchemaNameException) {
            status = PGErrorStatus.INVALID_SCHEMA_NAME;
        } else if (throwable instanceof AmbiguousColumnAliasException) {
            status = PGErrorStatus.AMBIGUOUS_ALIAS;
        } else if (throwable instanceof UserDefinedFunctionUnknownException) {
            status = PGErrorStatus.UNDEFINED_FUNCTION;
        } else if (throwable instanceof ElasticsearchException ex) {
            status = ex.pgErrorStatus();
        }
        return new PGError(status, SQLExceptions.messageOf(throwable), throwable);
    }
}

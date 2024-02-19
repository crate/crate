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

package io.crate.rest.action;

import static io.crate.common.exceptions.Exceptions.userFriendlyMessage;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.InvalidSnapshotNameException;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.jetbrains.annotations.Nullable;

import io.crate.common.exceptions.Exceptions;
import io.crate.exceptions.AmbiguousColumnAliasException;
import io.crate.exceptions.AmbiguousColumnException;
import io.crate.exceptions.AnalyzerInvalidException;
import io.crate.exceptions.AnalyzerUnknownException;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.DuplicateKeyException;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.exceptions.InvalidColumnNameException;
import io.crate.exceptions.InvalidRelationName;
import io.crate.exceptions.InvalidSchemaNameException;
import io.crate.exceptions.JobKilledException;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.exceptions.PartitionAlreadyExistsException;
import io.crate.exceptions.PartitionUnknownException;
import io.crate.exceptions.ReadOnlyException;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationValidationException;
import io.crate.exceptions.RelationsUnknown;
import io.crate.exceptions.RepositoryAlreadyExistsException;
import io.crate.exceptions.RoleAlreadyExistsException;
import io.crate.exceptions.RoleUnknownException;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.SQLParseException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.UnauthorizedException;
import io.crate.exceptions.UnavailableShardsException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.exceptions.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.UserDefinedFunctionUnknownException;
import io.crate.exceptions.VersioningValidationException;
import io.netty.handler.codec.http.HttpResponseStatus;

public class HttpError {

    private final HttpResponseStatus httpResponseStatus;
    private final int errorCode;
    private final String message;

    @Nullable
    private final Throwable t;

    public HttpError(HttpErrorStatus status, String message, @Nullable Throwable t) {
        this(status.httpResponseStatus(), status.errorCode(), message, t);
    }

    public HttpError(HttpResponseStatus httpResponseStatus, int errorCode, String message, @Nullable Throwable t) {
        this.httpResponseStatus = httpResponseStatus;
        this.errorCode = errorCode;
        this.message = message;
        this.t = t;
    }

    public HttpResponseStatus httpResponseStatus() {
        return httpResponseStatus;
    }

    public int errorCode() {
        return errorCode;
    }

    public String message() {
        return message;
    }

    public XContentBuilder toXContent(boolean includeErrorTrace) throws IOException {
        // @formatter:off
        XContentBuilder builder = JsonXContent.builder()
            .startObject()
            .startObject("error")
            .field("message", userFriendlyMessage(t))
            .field("code", errorCode)
            .endObject();
        // @formatter:on

        if (includeErrorTrace) {
            builder.field("error_trace", Exceptions.stackTrace(t));
        }
        return builder.endObject();
    }

    @Override
    public String toString() {
        return "HttpError{" +
               "httpResponseStatus=" + httpResponseStatus +
               ", errorCode=" + errorCode +
               ", message='" + message + '\'' +
               ", t=" + t +
               '}';
    }

    public static HttpError fromThrowable(Throwable throwable) {
        var httpErrorStatus = HttpErrorStatus.UNHANDLED_SERVER_ERROR;
        if (throwable instanceof MapperParsingException) {
            httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
        } else if (throwable instanceof MissingPrivilegeException) {
            httpErrorStatus = HttpErrorStatus.MISSING_USER_PRIVILEGES;
        } else if (throwable instanceof AmbiguousColumnAliasException) {
            httpErrorStatus = HttpErrorStatus.COLUMN_ALIAS_IS_AMBIGUOUS;
        } else if (throwable instanceof AmbiguousColumnException) {
            httpErrorStatus = HttpErrorStatus.COLUMN_ALIAS_IS_AMBIGUOUS;
        } else if (throwable instanceof AnalyzerInvalidException) {
            httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_ANALYZER_DEFINITION;
        } else if (throwable instanceof ColumnValidationException) {
            httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
        } else if (throwable instanceof InvalidArgumentException) {
            httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
        } else if (throwable instanceof InvalidColumnNameException) {
            httpErrorStatus = HttpErrorStatus.COLUMN_NAME_INVALID;
        } else if (throwable instanceof InvalidRelationName) {
            httpErrorStatus = HttpErrorStatus.RELATION_INVALID_NAME;
        } else if (throwable instanceof InvalidSchemaNameException) {
            httpErrorStatus = HttpErrorStatus.RELATION_INVALID_NAME;
        } else if (throwable instanceof OperationOnInaccessibleRelationException) {
            httpErrorStatus = HttpErrorStatus.RELATION_OPERATION_NOT_SUPPORTED;
        } else if (throwable instanceof RelationValidationException) {
            httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
        } else if (throwable instanceof SQLParseException) {
            httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
        } else if (throwable instanceof UnsupportedFeatureException ||
                    throwable instanceof UnsupportedFunctionException) {
            httpErrorStatus = HttpErrorStatus.POSSIBLE_FEATURE_NOT_SUPPROTED_YET;
        } else if (throwable instanceof VersioningValidationException) {
            httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
        } else if (throwable instanceof AnalyzerUnknownException) {
            httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_ANALYZER_DEFINITION;
        } else if (throwable instanceof ColumnUnknownException) {
            httpErrorStatus = HttpErrorStatus.COLUMN_UNKNOWN;
        } else if (throwable instanceof PartitionUnknownException) {
            httpErrorStatus = HttpErrorStatus.PARTITION_UNKNOWN;
        } else if (throwable instanceof RelationUnknown) {
            httpErrorStatus = HttpErrorStatus.RELATION_UNKNOWN;
        } else if (throwable instanceof RelationsUnknown) {
            httpErrorStatus = HttpErrorStatus.RELATION_UNKNOWN;
        } else if (throwable instanceof RepositoryMissingException) {
            httpErrorStatus = HttpErrorStatus.REPOSITORY_UNKNOWN;
        } else if (throwable instanceof SchemaUnknownException) {
            httpErrorStatus = HttpErrorStatus.SCHEMA_UNKNOWN;
        } else if (throwable instanceof UserDefinedFunctionUnknownException) {
            httpErrorStatus = HttpErrorStatus.USER_DEFINED_FUNCTION_UNKNOWN;
        } else if (throwable instanceof RoleUnknownException) {
            httpErrorStatus = HttpErrorStatus.USER_UNKNOWN;
        } else if (throwable instanceof SnapshotMissingException) {
            httpErrorStatus = HttpErrorStatus.SNAPSHOT_UNKNOWN;
        } else if (throwable instanceof UnauthorizedException) {
            httpErrorStatus = HttpErrorStatus.USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT;
        } else if (throwable instanceof ReadOnlyException) {
            httpErrorStatus = HttpErrorStatus.ONLY_READ_OPERATION_ALLOWED_ON_THIS_NODE;
        } else if (throwable instanceof DuplicateKeyException) {
            httpErrorStatus = HttpErrorStatus.DOCUMENT_WITH_THE_SAME_PRIMARY_KEY_EXISTS_ALREADY;
        } else if (throwable instanceof PartitionAlreadyExistsException) {
            httpErrorStatus = HttpErrorStatus.PARTITION_FOR_THE_SAME_VALUE_EXISTS_ALREADY;
        } else if (throwable instanceof RelationAlreadyExists) {
            httpErrorStatus = HttpErrorStatus.RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY;
        } else if (throwable instanceof RepositoryAlreadyExistsException) {
            httpErrorStatus = HttpErrorStatus.REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY;
        } else if (throwable instanceof InvalidSnapshotNameException) {
            httpErrorStatus = HttpErrorStatus.SNAPSHOT_WITH_SAME_NAME_EXISTS_ALREADY;
        } else if (throwable instanceof RoleAlreadyExistsException) {
            httpErrorStatus = HttpErrorStatus.USER_WITH_SAME_NAME_EXISTS_ALREADY;
        } else if (throwable instanceof UserDefinedFunctionAlreadyExistsException) {
            httpErrorStatus = HttpErrorStatus.USER_DEFINED_FUNCTION_WITH_SAME_SIGNATURE_EXISTS_ALREADY;
        } else if (throwable instanceof JobKilledException) {
            httpErrorStatus = HttpErrorStatus.QUERY_KILLED_BY_STATEMENT;
        } else if (throwable instanceof UnavailableShardsException) {
            httpErrorStatus = HttpErrorStatus.ONE_OR_MORE_SHARDS_NOT_AVAILABLE;
        } else if (throwable instanceof ElasticsearchException ex) {
            httpErrorStatus = ex.httpErrorStatus();
        }
        // Missing GroupByOnArrayUnsupportedException, SchemaScopeException, TaskMissing, UnhandledServerException
        // will be handled as UNHANDLED_SERVER_ERROR
        return new HttpError(httpErrorStatus, SQLExceptions.messageOf(throwable), throwable);
    }
}

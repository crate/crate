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

import io.crate.exceptions.scoped.table.AmbiguousColumnAliasException;
import io.crate.exceptions.scoped.table.AmbiguousColumnException;
import io.crate.exceptions.scoped.cluster.AnalyzerInvalidException;
import io.crate.exceptions.scoped.cluster.AnalyzerUnknownException;
import io.crate.exceptions.scoped.table.ColumnUnknownException;
import io.crate.exceptions.scoped.table.ColumnValidationException;
import io.crate.exceptions.CrateException;
import io.crate.exceptions.scoped.table.DuplicateKeyException;
import io.crate.exceptions.InvalidArgumentException;
import io.crate.exceptions.scoped.cluster.InvalidColumnNameException;
import io.crate.exceptions.unscoped.InvalidRelationName;
import io.crate.exceptions.scoped.schema.InvalidSchemaNameException;
import io.crate.exceptions.unscoped.JobKilledException;
import io.crate.exceptions.MissingPrivilegeException;
import io.crate.exceptions.scoped.table.OperationOnInaccessibleRelationException;
import io.crate.exceptions.scoped.table.PartitionAlreadyExistsException;
import io.crate.exceptions.scoped.table.PartitionUnknownException;
import io.crate.exceptions.scoped.cluster.ReadOnlyException;
import io.crate.exceptions.scoped.table.RelationAlreadyExists;
import io.crate.exceptions.scoped.table.RelationUnknown;
import io.crate.exceptions.scoped.table.RelationValidationException;
import io.crate.exceptions.scoped.table.RelationsUnknown;
import io.crate.exceptions.scoped.cluster.RepositoryAlreadyExistsException;
import io.crate.exceptions.scoped.cluster.RepositoryUnknownException;
import io.crate.exceptions.SQLExceptions;
import io.crate.exceptions.unscoped.SQLParseException;
import io.crate.exceptions.scoped.schema.SchemaUnknownException;
import io.crate.exceptions.scoped.cluster.SnapshotAlreadyExistsException;
import io.crate.exceptions.scoped.cluster.SnapshotNameInvalidException;
import io.crate.exceptions.scoped.cluster.SnapshotUnknownException;
import io.crate.exceptions.unscoped.UnauthorizedException;
import io.crate.exceptions.scoped.table.UnavailableShardsException;
import io.crate.exceptions.scoped.cluster.UnsupportedFeatureException;
import io.crate.exceptions.unscoped.UserAlreadyExistsException;
import io.crate.exceptions.scoped.schema.UserDefinedFunctionAlreadyExistsException;
import io.crate.exceptions.scoped.schema.UserDefinedFunctionUnknownException;
import io.crate.exceptions.unscoped.UserUnknownException;
import io.crate.exceptions.unscoped.VersioningValidationException;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperParsingException;

import javax.annotation.Nullable;
import java.io.IOException;

import static io.crate.exceptions.Exceptions.userFriendlyMessage;

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
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("error")
            .field("message", userFriendlyMessage(t))
            .field("code", errorCode)
            .endObject();
        // @formatter:on

        if (includeErrorTrace) {
            builder.field("error_trace", ExceptionsHelper.stackTrace(t));
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
            httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
        } else if (throwable instanceof CrateException) {
            CrateException crateException = (CrateException) throwable;
            if (crateException instanceof MissingPrivilegeException) {
                httpErrorStatus = HttpErrorStatus.MISSING_USER_PRIVILEGES;
            } else if (crateException instanceof AmbiguousColumnAliasException) {
                httpErrorStatus = HttpErrorStatus.COLUMN_ALIAS_IS_AMBIGUOUS;
            } else if (crateException instanceof AmbiguousColumnException) {
                httpErrorStatus = HttpErrorStatus.COLUMN_ALIAS_IS_AMBIGUOUS;
            } else if (crateException instanceof AnalyzerInvalidException) {
                httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_ANALYZER_DEFINITION;
            } else if (crateException instanceof ColumnValidationException) {
                httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
            } else if (crateException instanceof InvalidArgumentException) {
                httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
            } else if (crateException instanceof InvalidColumnNameException) {
                httpErrorStatus = HttpErrorStatus.COLUMN_NAME_INVALID;
            } else if (crateException instanceof InvalidRelationName) {
                httpErrorStatus = HttpErrorStatus.RELATION_INVALID_NAME;
            } else if (crateException instanceof InvalidSchemaNameException) {
                httpErrorStatus = HttpErrorStatus.RELATION_INVALID_NAME;
            } else if (crateException instanceof OperationOnInaccessibleRelationException) {
                httpErrorStatus = HttpErrorStatus.RELATION_OPERATION_NOT_SUPPORTED;
            } else if (crateException instanceof RelationValidationException) {
                httpErrorStatus = HttpErrorStatus.FIELD_VALIDATION_FAILED;
            } else if (crateException instanceof SQLParseException) {
                httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
            } else if (crateException instanceof UnsupportedFeatureException) {
                httpErrorStatus = HttpErrorStatus.POSSIBLE_FEATURE_NOT_SUPPROTED_YET;
            } else if (crateException instanceof VersioningValidationException) {
                httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
            } else if (crateException instanceof AnalyzerUnknownException) {
                httpErrorStatus = HttpErrorStatus.STATEMENT_INVALID_ANALYZER_DEFINITION;
            } else if (crateException instanceof ColumnUnknownException) {
                httpErrorStatus = HttpErrorStatus.COLUMN_UNKNOWN;
            } else if (crateException instanceof PartitionUnknownException) {
                httpErrorStatus = HttpErrorStatus.PARTITION_UNKNOWN;
            } else if (crateException instanceof RelationUnknown) {
                httpErrorStatus = HttpErrorStatus.RELATION_UNKNOWN;
            } else if (crateException instanceof RelationsUnknown) {
                httpErrorStatus = HttpErrorStatus.RELATION_UNKNOWN;
            } else if (crateException instanceof RepositoryUnknownException) {
                httpErrorStatus = HttpErrorStatus.REPOSITORY_UNKNOWN;
            } else if (crateException instanceof SchemaUnknownException) {
                httpErrorStatus = HttpErrorStatus.SCHEMA_UNKNOWN;
            } else if (crateException instanceof UserDefinedFunctionUnknownException) {
                httpErrorStatus = HttpErrorStatus.USER_DEFINED_FUNCTION_UNKNOWN;
            } else if (crateException instanceof UserUnknownException) {
                httpErrorStatus = HttpErrorStatus.USER_UNKNOWN;
            } else if (crateException instanceof SnapshotUnknownException) {
                httpErrorStatus = HttpErrorStatus.SNAPSHOT_UNKNOWN;
            } else if (crateException instanceof UnauthorizedException) {
                httpErrorStatus = HttpErrorStatus.USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT;
            } else if (crateException instanceof ReadOnlyException) {
                httpErrorStatus = HttpErrorStatus.ONLY_READ_OPERATION_ALLOWED_ON_THIS_NODE;
            } else if (crateException instanceof DuplicateKeyException) {
                httpErrorStatus = HttpErrorStatus.DOCUMENT_WITH_THE_SAME_PRIMARY_KEY_EXISTS_ALREADY;
            } else if (crateException instanceof PartitionAlreadyExistsException) {
                httpErrorStatus = HttpErrorStatus.PARTITION_FOR_THE_SAME_VALUE_EXISTS_ALREADY;
            } else if (crateException instanceof RelationAlreadyExists) {
                httpErrorStatus = HttpErrorStatus.RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY;
            } else if (crateException instanceof RepositoryAlreadyExistsException) {
                httpErrorStatus = HttpErrorStatus.REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY;
            } else if (crateException instanceof SnapshotAlreadyExistsException) {
                httpErrorStatus = HttpErrorStatus.SNAPSHOT_WITH_SAME_NAME_EXISTS_ALREADY;
            } else if (crateException instanceof SnapshotNameInvalidException) {
                httpErrorStatus = HttpErrorStatus.USER_WITH_SAME_NAME_EXISTS_ALREADY;
            } else if (crateException instanceof UserAlreadyExistsException) {
                httpErrorStatus = HttpErrorStatus.USER_WITH_SAME_NAME_EXISTS_ALREADY;
            } else if (crateException instanceof UserDefinedFunctionAlreadyExistsException) {
                httpErrorStatus = HttpErrorStatus.USER_DEFINED_FUNCTION_WITH_SAME_SIGNATURE_EXISTS_ALREADY;
            } else if (crateException instanceof JobKilledException) {
                httpErrorStatus = HttpErrorStatus.QUERY_KILLED_BY_STATEMENT;
            } else if (crateException instanceof UnavailableShardsException) {
                httpErrorStatus = HttpErrorStatus.ONE_OR_MORE_SHARDS_NOT_AVAILABLE;
            }
            // Missing GroupByOnArrayUnsupportedException, SchemaScopeException, TaskMissing, UnhandledServerException
            // will be handled as UNHANDLED_SERVER_ERROR
        }
        return new HttpError(httpErrorStatus, SQLExceptions.messageOf(throwable), throwable);
    }
}

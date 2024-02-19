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

import io.netty.handler.codec.http.HttpResponseStatus;

public enum HttpErrorStatus {

    STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX(HttpResponseStatus.BAD_REQUEST, 4000),
    STATEMENT_INVALID_ANALYZER_DEFINITION(HttpResponseStatus.BAD_REQUEST, 4001),
    RELATION_INVALID_NAME(HttpResponseStatus.BAD_REQUEST, 4002),
    FIELD_VALIDATION_FAILED(HttpResponseStatus.BAD_REQUEST,4003),
    POSSIBLE_FEATURE_NOT_SUPPROTED_YET(HttpResponseStatus.BAD_REQUEST,4004),
    ALTER_TABLE_USING_ALIAS_NOT_SUPPORTED(HttpResponseStatus.BAD_REQUEST, 4005),
    COLUMN_ALIAS_IS_AMBIGUOUS(HttpResponseStatus.BAD_REQUEST, 4006),
    RELATION_OPERATION_NOT_SUPPORTED(HttpResponseStatus.BAD_REQUEST,4007),
    COLUMN_NAME_INVALID(HttpResponseStatus.BAD_REQUEST,4008),
    USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT(HttpResponseStatus.UNAUTHORIZED, 4010),
    MISSING_USER_PRIVILEGES(HttpResponseStatus.UNAUTHORIZED, 4011),
    ONLY_READ_OPERATION_ALLOWED_ON_THIS_NODE(HttpResponseStatus.FORBIDDEN, 4031),
    RELATION_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4041),
    ANALYZER_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4042),
    COLUMN_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4043),
    TYPE_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4044),
    SCHEMA_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4045),
    PARTITION_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4046),
    REPOSITORY_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4047),
    SNAPSHOT_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4048),
    USER_DEFINED_FUNCTION_UNKNOWN(HttpResponseStatus.NOT_FOUND, 4049),
    USER_UNKNOWN(HttpResponseStatus.NOT_FOUND, 40410),
    DOCUMENT_WITH_THE_SAME_PRIMARY_KEY_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4091),
    VERSION_CONFLICT(HttpResponseStatus.CONFLICT,4092),
    RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4093),
    TABLE_ALIAS_CONTAINS_TABLES_WITH_DIFFERENT_SCHEMA(HttpResponseStatus.CONFLICT,4094),
    REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4095),
    SNAPSHOT_WITH_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4096),
    PARTITION_FOR_THE_SAME_VALUE_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4097),
    USER_DEFINED_FUNCTION_WITH_SAME_SIGNATURE_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4098),
    USER_WITH_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4099),
    DUPLICATE_OBJECT(HttpResponseStatus.CONFLICT, 4100),
    UNHANDLED_SERVER_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5000),
    EXECUTION_OF_TASK_FAILED(HttpResponseStatus.INTERNAL_SERVER_ERROR,5001),
    ONE_OR_MORE_SHARDS_NOT_AVAILABLE(HttpResponseStatus.INTERNAL_SERVER_ERROR,5002),
    QUERY_FAILED_ON_ONE_OR_MORE_SHARDS(HttpResponseStatus.INTERNAL_SERVER_ERROR,5003),
    CREATING_SNAPSHOT_FAILED(HttpResponseStatus.INTERNAL_SERVER_ERROR,5004),
    QUERY_KILLED_BY_STATEMENT(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5030);

    private final HttpResponseStatus httpResponseStatus;
    private final int errorCode;

    HttpErrorStatus(HttpResponseStatus httpResponseStatus, int erroCode) {
        this.httpResponseStatus = httpResponseStatus;
        this.errorCode = erroCode;
    }

    public HttpResponseStatus httpResponseStatus() {
        return httpResponseStatus;
    }

    public int errorCode() {
        return errorCode;
    }

}

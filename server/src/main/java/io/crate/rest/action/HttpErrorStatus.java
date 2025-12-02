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

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import com.carrotsearch.hppc.IntObjectHashMap;

import io.netty.handler.codec.http.HttpResponseStatus;

public enum HttpErrorStatus {

    OK(HttpResponseStatus.OK, 2000),

    /**
     * All BAD_REQUEST errors should be in the 4000-40099 range
     */
    GENERIC_BAD_REQUEST(HttpResponseStatus.BAD_REQUEST, 40000), // should be 4000, but is already taken
    STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX(HttpResponseStatus.BAD_REQUEST, 4000),
    STATEMENT_INVALID_ANALYZER_DEFINITION(HttpResponseStatus.BAD_REQUEST, 4001),
    RELATION_INVALID_NAME(HttpResponseStatus.BAD_REQUEST, 4002),
    FIELD_VALIDATION_FAILED(HttpResponseStatus.BAD_REQUEST,4003),
    POSSIBLE_FEATURE_NOT_SUPPORTED_YET(HttpResponseStatus.BAD_REQUEST,4004),
    ALTER_TABLE_USING_ALIAS_NOT_SUPPORTED(HttpResponseStatus.BAD_REQUEST, 4005),
    COLUMN_ALIAS_IS_AMBIGUOUS(HttpResponseStatus.BAD_REQUEST, 4006),
    RELATION_OPERATION_NOT_SUPPORTED(HttpResponseStatus.BAD_REQUEST,4007),
    COLUMN_NAME_INVALID(HttpResponseStatus.BAD_REQUEST,4008),
    // 4009 was "CrateDB License required"
    DOCUMENT_SOURCE_MISSING(HttpResponseStatus.BAD_REQUEST,40010),
    SNAPSHOT_INVALID_NAME(HttpResponseStatus.BAD_REQUEST,40011),
    SNAPSHOT_IN_PROGRESS(HttpResponseStatus.BAD_REQUEST,40012),
    DEPENDENT_OBJECTS_STILL_EXIST(HttpResponseStatus.BAD_REQUEST, 40013),

    /**
     * All UNAUTHORIZED errors should be in the 4010-40199 range
     */
    USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT(HttpResponseStatus.UNAUTHORIZED, 4010),
    MISSING_USER_PRIVILEGES(HttpResponseStatus.UNAUTHORIZED, 4011),

    /**
     * All FORBIDDEN errors should be in the 4030-40399 range
     */
    GENERIC_FORBIDDEN(HttpResponseStatus.FORBIDDEN, 4030),
    ONLY_READ_OPERATION_ALLOWED_ON_THIS_NODE(HttpResponseStatus.FORBIDDEN, 4031),
    RELATION_CLOSED(HttpResponseStatus.FORBIDDEN, 4032),
    RELATION_READ_ONLY(HttpResponseStatus.FORBIDDEN, 4033),
    RELATION_WRITE_ONLY(HttpResponseStatus.FORBIDDEN, 4034),
    RELATION_READ_AND_DDL_ONLY(HttpResponseStatus.FORBIDDEN, 4035),
    RELATION_DDL_ONLY(HttpResponseStatus.FORBIDDEN, 4036),
    RELATION_READ_DELETE_ONLY(HttpResponseStatus.FORBIDDEN, 4037),

    /**
     * All NOT_FOUND errors should be in the 4040-40499 range
     */
    GENERIC_NOT_FOUND(HttpResponseStatus.NOT_FOUND, 4040),
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
    DOCUMENT_NOT_FOUND(HttpResponseStatus.NOT_FOUND, 40411),

    /**
     * All CONFLICT errors should be in the 4090-40999 range
     */
    GENERIC_CONFLICT(HttpResponseStatus.CONFLICT,4090),
    DOCUMENT_WITH_THE_SAME_PRIMARY_KEY_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4091),
    VERSION_CONFLICT(HttpResponseStatus.CONFLICT,4092),
    RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4093),
    TABLE_ALIAS_CONTAINS_TABLES_WITH_DIFFERENT_SCHEMA(HttpResponseStatus.CONFLICT,4094),
    REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4095),
    SNAPSHOT_WITH_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4096),
    PARTITION_FOR_THE_SAME_VALUE_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4097),
    USER_DEFINED_FUNCTION_WITH_SAME_SIGNATURE_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4098),
    USER_WITH_SAME_NAME_EXISTS_ALREADY(HttpResponseStatus.CONFLICT,4099),
    DUPLICATE_OBJECT(HttpResponseStatus.CONFLICT, 40910),

    TOO_MANY_REQUESTS(HttpResponseStatus.TOO_MANY_REQUESTS, 4290),

    /**
     * All INTERNAL_SERVER_ERROR errors should be in the 5000-50099 range
     */
    UNHANDLED_SERVER_ERROR(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5000),
    EXECUTION_OF_TASK_FAILED(HttpResponseStatus.INTERNAL_SERVER_ERROR,5001),
    ONE_OR_MORE_SHARDS_NOT_AVAILABLE(HttpResponseStatus.INTERNAL_SERVER_ERROR,5002),
    QUERY_FAILED_ON_ONE_OR_MORE_SHARDS(HttpResponseStatus.INTERNAL_SERVER_ERROR,5003),
    CREATING_SNAPSHOT_FAILED(HttpResponseStatus.INTERNAL_SERVER_ERROR,5004),
    QUERY_KILLED_BY_STATEMENT(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5005),
    REPOSITORY_VERIFICATION_FAILED(HttpResponseStatus.INTERNAL_SERVER_ERROR, 5006),

    /**
     * All SERVICE_UNAVAILABLE errors should be in the 5030-50399 range
     */
    SERVICE_UNAVAILABLE(HttpResponseStatus.SERVICE_UNAVAILABLE, 5030),
    MASTER_NOT_DISCOVERED(HttpResponseStatus.SERVICE_UNAVAILABLE, 5031),
    NO_NODE_AVAILABLE(HttpResponseStatus.SERVICE_UNAVAILABLE, 5032),
    NO_SHARD_AVAILABLE(HttpResponseStatus.SERVICE_UNAVAILABLE, 5033),
    PROCESS_CLUSTER_EVENT_TIMEOUT(HttpResponseStatus.SERVICE_UNAVAILABLE, 5034),
    STATE_NOT_RECOVERED(HttpResponseStatus.SERVICE_UNAVAILABLE, 5035);

    private static final IntObjectHashMap<HttpErrorStatus> CODE_TO_STATUS;

    static {
        HttpErrorStatus[] values = values();
        IntObjectHashMap<HttpErrorStatus> codeToStatus = new IntObjectHashMap<>(values.length);
        for (HttpErrorStatus value : values) {
            codeToStatus.put(value.errorCode, value);
        }
        CODE_TO_STATUS = codeToStatus;
    }

    private final HttpResponseStatus httpResponseStatus;
    private final int errorCode;


    HttpErrorStatus(HttpResponseStatus httpResponseStatus, int errorCode) {
        this.httpResponseStatus = httpResponseStatus;
        this.errorCode = errorCode;
    }

    public HttpResponseStatus httpResponseStatus() {
        return httpResponseStatus;
    }

    public int errorCode() {
        return errorCode;
    }

    public static HttpErrorStatus readFrom(StreamInput in) throws IOException {
        return in.readEnum(HttpErrorStatus.class);
    }

    public static void writeTo(StreamOutput out, HttpErrorStatus status) throws IOException {
        out.writeEnum(status);
    }
}

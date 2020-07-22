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

package io.crate.rest.action;

public enum HttpErrorStatus {

    STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX(4000, "The statement contains an invalid syntax or unsupported SQL statement"),
    STATEMENT_INVALID_ANALYZER_DEFINITION(4001, "The statement contains an invalid analyzer definition."),
    RELATION_INVALID_NAME(4002, "The name of the relation is invalid"),
    FIELD_VALIDATION_FAILED(4003, "Field type validation failed"),
    POSSIBLE_FEATURE_NOT_SUPPROTED_YET(4004, "Possible feature not supported (yet)"),
    ALTER_TABLE_USING_ALIAS_NOT_SUPPORTED(4005, "Alter table using a table alias is not supported."),
    COLUMN_ALIAS_IS_AMBIGUOUS(4006, "Alter table using a table alias is not supported."),
    RELATION_OPERATION_NOT_SUPPORTED(4007, "The operation is not supported on this relation, as it is not accessible"),
    COLUMN_NAME_INVALID(4008, "The name of the column is invalid"),
    LICENSE_EXPIRED(4009, "CrateDB License is expired"),
    USER_NOT_AUTHORIZED_TO_PERFORM_STATEMENT(4010, "User is not authorized to perform the SQL statement"),
    MISSING_USER_PRIVILEGES(4011, "Missing privilege for user"),
    ONLY_READ_OPERATION_ALLOWED_ON_THIS_NODE(4031, "Only read operations are allowed on this node"),
    UNKNOWN_RELATION(4041, "Unknown Relation"),
    UNKNOWN_ANALYZER(4042, "Unknown Analyzer"),
    UNKNOWN_COLUMN(4043, "Unknown Column"),
    UNKNOWN_TYPE(4044, "Unknown Type"),
    UNKNOWN_SCHEMA(4045, "Unknown Schema"),
    UNKNOWN_PARTITION(4046, "Unknown Partition"),
    UNKNOWN_REPOSITORY(4047, "Unknown Repository"),
    UNKNOWN_SNAPSHOT(4048, "Unknown Snapshot"),
    UNKNOWN_USER_DEFINED_FUNCTION(4049, "Unknown user-defined function"),
    UNKNONW_USER(40410, "Unknown user"),
    DOCUMENT_WITH_THE_SAME_PRIMARY_KEY_EXISTS_ALREADY(4091, "A document with the same primary key exists already"),
    VERSION_CONFLICT(4092, "A VersionConflict. Might be thrown if an attempt was made to update the same document concurrently"),
    RELATION_WITH_THE_SAME_NAME_EXISTS_ALREADY(4093, "A relation with the same name exists already"),
    TABLE_ALIAS_CONTAINS_TABLES_WITH_DIFFERENT_SCHEMA(4094, "The used table alias contains tables with different schema"),
    REPOSITORY_WITH_SAME_NAME_EXISTS_ALREADY(4095, "A repository with the same name exists already"),
    SNAPSHOT_WITH_SAME_NAME_EXISTS_ALREADY(4096, "A snapshot with the same name already exists in the repository"),
    PARTITION_FOR_THE_SAME_VALUE_EXISTS_ALREADY(4097, "A partition for the same values already exists in this table"),
    USER_DEFINED_FUNCTION_WITH_SAME_SIGNATURE_EXISTS_ALREADY(4098, "A user-defined function with the same signature already exists"),
    USER_WITH_SAME_NAME_EXISTS_ALREADY(4099, "A user with the same name already exists"),
    UNHANDLED_SERVER_ERROR(5000, "Unhandled server error"),
    EXECUTION_OF_TASK_FAILED(5001, "The execution of one or more tasks failed"),
    ONE_OR_MORE_SHARDS_NOT_AVAILABLE(5002, "One or more shards are not available"),
    QUERY_FAILED_ON_ONE_OR_MORE_SHARDS(5003, "The query failed on one or more shards"),
    CREATING_SNAPSHOT_FAILED(5004, "Creating a snapshot failed"),
    QUERY_KILLED_BY_STATEMENT(5004, "The query was killed by a kill statement");

    public final int errorCode;
    public final String message;

    HttpErrorStatus(int erroCode, String message) {
        this.errorCode = erroCode;
        this.message = message;
    }
}

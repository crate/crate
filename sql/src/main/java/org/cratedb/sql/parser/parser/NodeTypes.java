/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* The original from which this derives bore the following: */

/*

   Derby - Class org.apache.derby.iapi.sql.compile.C_NodeTypes

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.cratedb.sql.parser.parser;

/**
 * The purpose of this interface is to hold the constant definitions
 * of the different node type identifiers, for use with NodeFactory.
 * The reason this class exists is that it is not shipped with the
 * product, so it saves footprint to have all these constant definitions
 * here instead of in NodeFactory.
 */
public interface NodeTypes
{
    /** Node types, for use with getNode methods */
    public static final int TEST_CONSTRAINT_NODE = 1;
    public static final int CURRENT_ROW_LOCATION_NODE = 2;
    public static final int GROUP_BY_LIST = 3;
    public static final int CURRENT_ISOLATION_NODE = 4;
    public static final int IDENTITY_VAL_NODE = 5;
    public static final int CURRENT_SCHEMA_NODE = 6;
    public static final int ORDER_BY_LIST = 7;
    public static final int PREDICATE_LIST = 8;
    public static final int RESULT_COLUMN_LIST = 9;
    public static final int INDEX_COLUMN = 10;
    public static final int SUBQUERY_LIST = 11;
    public static final int TABLE_ELEMENT_LIST = 12;
    public static final int UNTYPED_NULL_CONSTANT_NODE = 13;
    public static final int TABLE_ELEMENT_NODE = 14;
    public static final int VALUE_NODE_LIST = 15;
    public static final int ALL_RESULT_COLUMN = 16;
    public static final int INDEX_COLUMN_LIST = 17;
    public static final int GET_CURRENT_CONNECTION_NODE = 18;
    public static final int NOP_STATEMENT_NODE = 19;
    public static final int OCTET_LENGTH_OPERATOR_NODE = 20;
    public static final int SET_TRANSACTION_ISOLATION_NODE = 21;
    public static final int SET_TRANSACTION_ACCESS_NODE = 22;
    public static final int CHAR_LENGTH_OPERATOR_NODE = 23;
    public static final int IS_NOT_NULL_NODE = 24;
    public static final int IS_NULL_NODE = 25;
    public static final int NOT_NODE = 26;
    public static final int SET_CONFIGURATION_NODE = 27;
    public static final int SQL_TO_JAVA_VALUE_NODE = 28;
    public static final int UNARY_MINUS_OPERATOR_NODE = 29;
    public static final int UNARY_PLUS_OPERATOR_NODE = 30;
    public static final int SQL_BOOLEAN_CONSTANT_NODE = 31;
    public static final int UNARY_DATE_TIMESTAMP_OPERATOR_NODE = 32;
    public static final int TIMESTAMP_OPERATOR_NODE = 33;
    public static final int TABLE_NAME = 34;
    public static final int GROUP_BY_COLUMN = 35;
    public static final int JAVA_TO_SQL_VALUE_NODE = 36;
    public static final int FROM_LIST = 37;
    public static final int BOOLEAN_CONSTANT_NODE = 38;
    public static final int AND_NODE = 39;
    public static final int BINARY_DIVIDE_OPERATOR_NODE = 40;
    public static final int BINARY_EQUALS_OPERATOR_NODE = 41;
    public static final int BINARY_GREATER_EQUALS_OPERATOR_NODE = 42;
    public static final int BINARY_GREATER_THAN_OPERATOR_NODE = 43;
    public static final int BINARY_LESS_EQUALS_OPERATOR_NODE = 44;
    public static final int BINARY_LESS_THAN_OPERATOR_NODE = 45;
    public static final int BINARY_MINUS_OPERATOR_NODE = 46;
    public static final int BINARY_NOT_EQUALS_OPERATOR_NODE = 47;
    public static final int BINARY_PLUS_OPERATOR_NODE = 48;
    public static final int BINARY_TIMES_OPERATOR_NODE = 49;
    public static final int CONCATENATION_OPERATOR_NODE = 50;
    public static final int LIKE_OPERATOR_NODE = 51;
    public static final int OR_NODE = 52;
    public static final int BETWEEN_OPERATOR_NODE = 53;
    public static final int CONDITIONAL_NODE = 54;
    public static final int IN_LIST_OPERATOR_NODE = 55;
    public static final int NOT_BETWEEN_OPERATOR_NODE = 56;
    public static final int NOT_IN_LIST_OPERATOR_NODE = 57;
    public static final int BIT_CONSTANT_NODE = 58;
    public static final int VARBIT_CONSTANT_NODE = 59;
    public static final int CAST_NODE = 60;
    public static final int CHAR_CONSTANT_NODE = 61;
    public static final int COLUMN_REFERENCE = 62;
    public static final int DROP_INDEX_NODE = 63;
    public static final int UNARY_BITNOT_OPERATOR_NODE = 64;
    public static final int DROP_TRIGGER_NODE = 65;
    public static final int BINARY_BIT_OPERATOR_NODE = 66;
    public static final int DECIMAL_CONSTANT_NODE = 67;
    public static final int DOUBLE_CONSTANT_NODE = 68;
    public static final int FLOAT_CONSTANT_NODE = 69;
    public static final int INT_CONSTANT_NODE = 70;
    public static final int LONGINT_CONSTANT_NODE = 71;
    public static final int LONGVARBIT_CONSTANT_NODE = 72;
    public static final int LONGVARCHAR_CONSTANT_NODE = 73;
    public static final int SMALLINT_CONSTANT_NODE = 74;
    public static final int TINYINT_CONSTANT_NODE = 75;
    public static final int USERTYPE_CONSTANT_NODE = 76;
    public static final int VARCHAR_CONSTANT_NODE = 77;
    public static final int PREDICATE = 78;
    public static final int BINARY_DIV_OPERATOR_NODE = 79;
    public static final int RESULT_COLUMN = 80;
    public static final int SET_SCHEMA_NODE = 81;
    public static final int UPDATE_COLUMN = 82;
    public static final int SIMPLE_STRING_OPERATOR_NODE = 83;
    public static final int STATIC_CLASS_FIELD_REFERENCE_NODE = 84;
    public static final int STATIC_METHOD_CALL_NODE = 85;
    public static final int REVOKE_NODE = 86;
    public static final int EXTRACT_OPERATOR_NODE = 87;
    public static final int PARAMETER_NODE = 88;
    public static final int GRANT_NODE = 89;
    public static final int DROP_SCHEMA_NODE = 90;
    public static final int DROP_TABLE_NODE = 91;
    public static final int DROP_VIEW_NODE = 92;
    public static final int SUBQUERY_NODE = 93;
    public static final int BASE_COLUMN_NODE = 94;
    public static final int CALL_STATEMENT_NODE = 95;
    public static final int MODIFY_COLUMN_DEFAULT_NODE = 97;
    public static final int NON_STATIC_METHOD_CALL_NODE = 98;
    public static final int CURRENT_OF_NODE = 99;
    public static final int DEFAULT_NODE = 100;
    public static final int DELETE_NODE = 101;
    public static final int UPDATE_NODE = 102;
    public static final int PRIVILEGE_NODE = 103;
    public static final int ORDER_BY_COLUMN = 104;
    public static final int ROW_RESULT_SET_NODE = 105;
    public static final int TABLE_PRIVILEGES_NODE = 106;
    public static final int VIRTUAL_COLUMN_NODE = 107;
    public static final int CURRENT_DATETIME_OPERATOR_NODE = 108;
    public static final int CURRENT_USER_NODE = 109; // special function CURRENT_USER
    public static final int USER_NODE = 110; // // special function USER
    public static final int IS_NODE = 111;
    public static final int LOCK_TABLE_NODE = 112;
    public static final int DROP_COLUMN_NODE = 113;
    public static final int ALTER_TABLE_NODE = 114;
    public static final int AGGREGATE_NODE = 115;
    public static final int COLUMN_DEFINITION_NODE = 116;
    public static final int EXPLAIN_STATEMENT_NODE = 117;
    public static final int COPY_STATEMENT_NODE = 118;
    public static final int FK_CONSTRAINT_DEFINITION_NODE = 119;
    public static final int FROM_VTI = 120;
    public static final int MATERIALIZE_RESULT_SET_NODE = 121;
    public static final int NORMALIZE_RESULT_SET_NODE = 122;
    public static final int SCROLL_INSENSITIVE_RESULT_SET_NODE = 123;
    public static final int DISTINCT_NODE = 124;
    public static final int SESSION_USER_NODE = 125; // // special function SESSION_USER
    public static final int SYSTEM_USER_NODE = 126; // // special function SYSTEM_USER
    public static final int TRIM_OPERATOR_NODE = 127;
    public static final int INDEX_HINT_NODE = 128;
    public static final int SELECT_NODE = 129;
    public static final int CREATE_VIEW_NODE = 130;
    public static final int CONSTRAINT_DEFINITION_NODE = 131;
    public static final int INDEX_HINT_LIST = 132;
    public static final int NEW_INVOCATION_NODE = 133;
    public static final int CREATE_SCHEMA_NODE = 134;
    public static final int FROM_BASE_TABLE = 135;
    public static final int FROM_SUBQUERY = 136;
    public static final int GROUP_BY_NODE = 137;
    public static final int INSERT_NODE = 138;
    public static final int JOIN_NODE = 139;
    public static final int ORDER_BY_NODE = 140;
    public static final int CREATE_TABLE_NODE = 141;
    public static final int UNION_NODE = 142;
    public static final int CREATE_TRIGGER_NODE = 143;
    public static final int HALF_OUTER_JOIN_NODE = 144;
    public static final int EXPLICIT_COLLATE_NODE = 145;
    public static final int CREATE_INDEX_NODE = 146;
    public static final int CURSOR_NODE = 147;
    public static final int HASH_TABLE_NODE = 148;
    public static final int INDEX_TO_BASE_ROW_NODE = 149;
    public static final int CREATE_ALIAS_NODE = 150;
    public static final int PROJECT_RESTRICT_NODE = 151;
    // UNUSED public static final int BOOLEAN_TRUE_NODE = 152;
    // UNUSED public static final int BOOLEAN_FALSE_NODE = 153;
    public static final int SUBSTRING_OPERATOR_NODE = 154;
    // UNUSED public static final int BOOLEAN_NODE = 155;
    public static final int DROP_ALIAS_NODE = 156;
    public static final int INTERSECT_OR_EXCEPT_NODE = 157;
    public static final int LEFT_FN_NODE = 158;
    public static final int RIGHT_FN_NODE = 159;
    public static final int ROWS_RESULT_SET_NODE = 160;
    // UNUSED public static final int SPECIAL_INDEX_FUNC_NODE = 161;
    public static final int AT_DROP_INDEX_NODE = 162;
    public static final int AT_ADD_INDEX_NODE = 163;
    public static final int INDEX_CONSTRAINT_NODE = 164;
    public static final int DROP_GROUP_NODE = 165;
    public static final int ROW_CTOR_NODE = 166;
    public static final int GROUP_CONCAT_NODE = 167;
    public static final int AT_RENAME_NODE = 168;
    public static final int AT_RENAME_COLUMN_NODE = 169;
    public static final int SIMPLE_CASE_NODE = 170;
    public static final int PARTITION_BY_LIST = 171;
    public static final int PARTITION_BY_COLUMN = 172;
    public static final int FULL_OUTER_JOIN_NODE = 173;
    // 174 - 182 available
    public static final int ALTER_SERVER_NODE = 183;
    public static final int TIMESTAMP_ADD_FN_NODE = 184;
    public static final int TIMESTAMP_DIFF_FN_NODE = 185;
    public static final int MODIFY_COLUMN_TYPE_NODE = 186;
    public static final int MODIFY_COLUMN_CONSTRAINT_NODE = 187;
    public static final int ABSOLUTE_OPERATOR_NODE = 188;
    public static final int SQRT_OPERATOR_NODE = 189;
    public static final int LOCATE_FUNCTION_NODE = 190;
    //for rename table/column/index
    public static final int RENAME_NODE = 191;

    public static final int COALESCE_FUNCTION_NODE = 192;

    public static final int MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE = 193;

    public static final int MOD_OPERATOR_NODE = 194;
    // LOB
    public static final int BLOB_CONSTANT_NODE = 195;
    public static final int CLOB_CONSTANT_NODE = 196;

    // Transactions / savepoints
    public static final int TRANSACTION_CONTROL_NODE = 197;
    public static final int SAVEPOINT_NODE = 198;

    // XML
    public static final int XML_CONSTANT_NODE = 199;
    public static final int XML_PARSE_OPERATOR_NODE = 200;
    public static final int XML_SERIALIZE_OPERATOR_NODE = 201;
    public static final int XML_EXISTS_OPERATOR_NODE = 202;
    public static final int XML_QUERY_OPERATOR_NODE = 203;

    // Roles
    public static final int CURRENT_ROLE_NODE = 210;
    public static final int CREATE_ROLE_NODE = 211;
    public static final int SET_ROLE_NODE = 212;
    public static final int SET_ROLE_DYNAMIC = 213;
    public static final int DROP_ROLE_NODE = 214;
    public static final int GRANT_ROLE_NODE = 215;
    public static final int REVOKE_ROLE_NODE = 216;

    // generated columns
    public static final int GENERATION_CLAUSE_NODE = 222;

    // OFFSET, FETCH FIRST node
    public static final int ROW_COUNT_NODE = 223;

    // sequences
    public static final int CREATE_SEQUENCE_NODE = 224;
    public static final int DROP_SEQUENCE_NODE = 225;
    public static final int NEXT_SEQUENCE_NODE = 231;
    public static final int CURRENT_SEQUENCE_NODE = 232;

    // Windowing
    public static final int AGGREGATE_WINDOW_FUNCTION_NODE = 226;
    public static final int ROW_NUMBER_FUNCTION_NODE = 227;
    public static final int WINDOW_DEFINITION_NODE = 228;
    public static final int WINDOW_REFERENCE_NODE = 229;
    public static final int WINDOW_RESULTSET_NODE = 230;

    // Cursors
    public static final int DECLARE_STATEMENT_NODE = 233;
    public static final int FETCH_STATEMENT_NODE = 234;
    public static final int CLOSE_STATEMENT_NODE = 235;
    public static final int PREPARE_STATEMENT_NODE = 236;
    public static final int EXECUTE_STATEMENT_NODE = 237;
    public static final int DEALLOCATE_STATEMENT_NODE = 238;

    // Final value in set, keep up to date!
    public static final int FINAL_VALUE = DEALLOCATE_STATEMENT_NODE;

    /**
     * Extensions to this interface can use nodetypes > MAX_NODE_TYPE with out fear of collision
     * with C_NodeTypes
     */
    public static final int MAX_NODE_TYPE = 999;
}

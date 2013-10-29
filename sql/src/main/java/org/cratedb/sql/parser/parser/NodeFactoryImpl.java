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

   Derby - Class org.apache.derby.impl.sql.compile.NodeFactoryImpl

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

import org.cratedb.sql.parser.StandardException;

/**
    Create new parser AST nodes.
    <p>
    There is one of these per parser context, possibly wrapped for higher-level uses.
 */

public final class NodeFactoryImpl extends NodeFactory
{
    private static class ClassInfo {
        Class clazz;

        ClassInfo(Class clazz) {
            this.clazz = clazz;
        }

        // TODO: The Derby version optimized this by caching the
        // Method. Is that still necessary?
        Object getNewInstance() throws InstantiationException, IllegalAccessException {
            return clazz.newInstance();
        }
    }

    private final ClassInfo[] nodeCi = new ClassInfo[NodeTypes.FINAL_VALUE+1];

    /**
     * Get a node that takes no initializer arguments.
     *
     * @param nodeType Identifier for the type of node.
     * @param pc A SQLParserContext
     *
     * @return A new QueryTree node.
     *
     * @exception StandardException Thrown on error.
     */
    public QueryTreeNode getNode(int nodeType, SQLParserContext pc)
            throws StandardException {

        ClassInfo ci = nodeCi[nodeType];

        Class nodeClass = null;
        if (ci == null) {
            String nodeName = nodeName(nodeType);

            try {
                nodeClass = Class.forName(nodeName);
            }
            catch (ClassNotFoundException cnfe) {
                throw new StandardException(cnfe);
            }

            ci = new ClassInfo(nodeClass);
            nodeCi[nodeType] = ci;
        }

        QueryTreeNode retval = null;

        try {
            retval = (QueryTreeNode)ci.getNewInstance();
        }
        catch (Exception iae) {
            throw new StandardException(iae);
        }

        retval.setParserContext(pc);
        retval.setNodeType(nodeType);

        return retval;
    }

    /**
     * Translate a node type from NodeTypes to a class name
     *
     * @param nodeType A node type identifier from NodeTypes
     *
     * @exception StandardException Thrown on error
     */
    protected String nodeName(int nodeType) throws StandardException {
        switch (nodeType) {
        case NodeTypes.CURRENT_ROW_LOCATION_NODE:
            return NodeNames.CURRENT_ROW_LOCATION_NODE_NAME;

        case NodeTypes.GROUP_BY_LIST:
            return NodeNames.GROUP_BY_LIST_NAME;

        case NodeTypes.ORDER_BY_LIST:
            return NodeNames.ORDER_BY_LIST_NAME;

        case NodeTypes.PREDICATE_LIST:
            return NodeNames.PREDICATE_LIST_NAME;

        case NodeTypes.RESULT_COLUMN_LIST:
            return NodeNames.RESULT_COLUMN_LIST_NAME;

        case NodeTypes.SUBQUERY_LIST:
            return NodeNames.SUBQUERY_LIST_NAME;

        case NodeTypes.TABLE_ELEMENT_LIST:
            return NodeNames.TABLE_ELEMENT_LIST_NAME;

        case NodeTypes.UNTYPED_NULL_CONSTANT_NODE:
            return NodeNames.UNTYPED_NULL_CONSTANT_NODE_NAME;

        case NodeTypes.TABLE_ELEMENT_NODE:
            return NodeNames.TABLE_ELEMENT_NODE_NAME;

        case NodeTypes.VALUE_NODE_LIST:
            return NodeNames.VALUE_NODE_LIST_NAME;

        case NodeTypes.ALL_RESULT_COLUMN:
            return NodeNames.ALL_RESULT_COLUMN_NAME;

        case NodeTypes.GET_CURRENT_CONNECTION_NODE:
            return NodeNames.GET_CURRENT_CONNECTION_NODE_NAME;

        case NodeTypes.NOP_STATEMENT_NODE:
            return NodeNames.NOP_STATEMENT_NODE_NAME;

        case NodeTypes.SET_TRANSACTION_ACCESS_NODE:
            return NodeNames.SET_TRANSACTION_ACCESS_NODE_NAME;

        case NodeTypes.SET_TRANSACTION_ISOLATION_NODE:
            return NodeNames.SET_TRANSACTION_ISOLATION_NODE_NAME;

        case NodeTypes.CHAR_LENGTH_OPERATOR_NODE:
            return NodeNames.LENGTH_OPERATOR_NODE_NAME;

            // ISNOTNULL compressed into ISNULL
        case NodeTypes.IS_NOT_NULL_NODE:
        case NodeTypes.IS_NULL_NODE:
            return NodeNames.IS_NULL_NODE_NAME;

        case NodeTypes.NOT_NODE:
            return NodeNames.NOT_NODE_NAME;

        case NodeTypes.SET_CONFIGURATION_NODE:
            return NodeNames.SET_CONFIGURATION_NODE_NAME;

        case NodeTypes.SQL_TO_JAVA_VALUE_NODE:
            return NodeNames.SQL_TO_JAVA_VALUE_NODE_NAME;

        case NodeTypes.TABLE_NAME:
            return NodeNames.TABLE_NAME_NAME;

        case NodeTypes.GROUP_BY_COLUMN:
            return NodeNames.GROUP_BY_COLUMN_NAME;

        case NodeTypes.JAVA_TO_SQL_VALUE_NODE:
            return NodeNames.JAVA_TO_SQL_VALUE_NODE_NAME;

        case NodeTypes.FROM_LIST:
            return NodeNames.FROM_LIST_NAME;

        case NodeTypes.BOOLEAN_CONSTANT_NODE:
            return NodeNames.BOOLEAN_CONSTANT_NODE_NAME;

        case NodeTypes.AND_NODE:
            return NodeNames.AND_NODE_NAME;
        
        case NodeTypes.TRIM_OPERATOR_NODE:
            return NodeNames.TRIM_OPERATOR_NODE_NAME;
            
        case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
        case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
        case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
        case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
        case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
        case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
            return NodeNames.BINARY_RELATIONAL_OPERATOR_NODE_NAME;

        case NodeTypes.BINARY_DIV_OPERATOR_NODE:
        case NodeTypes.BINARY_MINUS_OPERATOR_NODE:
        case NodeTypes.BINARY_PLUS_OPERATOR_NODE:
        case NodeTypes.BINARY_TIMES_OPERATOR_NODE:
        case NodeTypes.BINARY_DIVIDE_OPERATOR_NODE:
        case NodeTypes.MOD_OPERATOR_NODE:
            return NodeNames.BINARY_ARITHMETIC_OPERATOR_NODE_NAME;

        case NodeTypes.COALESCE_FUNCTION_NODE:
            return NodeNames.COALESCE_FUNCTION_NODE_NAME;

        case NodeTypes.CONCATENATION_OPERATOR_NODE:
            return NodeNames.CONCATENATION_OPERATOR_NODE_NAME;

        case NodeTypes.LIKE_OPERATOR_NODE:
            return NodeNames.LIKE_OPERATOR_NODE_NAME;

        case NodeTypes.OR_NODE:
            return NodeNames.OR_NODE_NAME;

        case NodeTypes.BETWEEN_OPERATOR_NODE:
            return NodeNames.BETWEEN_OPERATOR_NODE_NAME;

        case NodeTypes.CONDITIONAL_NODE:
            return NodeNames.CONDITIONAL_NODE_NAME;

        case NodeTypes.IN_LIST_OPERATOR_NODE:
            return NodeNames.IN_LIST_OPERATOR_NODE_NAME;

        case NodeTypes.BIT_CONSTANT_NODE:
            return NodeNames.BIT_CONSTANT_NODE_NAME;

        case NodeTypes.LONGVARBIT_CONSTANT_NODE:
        case NodeTypes.VARBIT_CONSTANT_NODE:
        case NodeTypes.BLOB_CONSTANT_NODE:
            return NodeNames.VARBIT_CONSTANT_NODE_NAME;

        case NodeTypes.CAST_NODE:
            return NodeNames.CAST_NODE_NAME;

        case NodeTypes.CHAR_CONSTANT_NODE:
        case NodeTypes.LONGVARCHAR_CONSTANT_NODE:
        case NodeTypes.VARCHAR_CONSTANT_NODE:
        case NodeTypes.CLOB_CONSTANT_NODE:
            return NodeNames.CHAR_CONSTANT_NODE_NAME;

        case NodeTypes.XML_CONSTANT_NODE:
            return NodeNames.XML_CONSTANT_NODE_NAME;

        case NodeTypes.COLUMN_REFERENCE:
            return NodeNames.COLUMN_REFERENCE_NAME;

        case NodeTypes.DROP_INDEX_NODE:
            return NodeNames.DROP_INDEX_NODE_NAME;

        case NodeTypes.UNARY_BITNOT_OPERATOR_NODE:
            return NodeNames.UNARY_BIT_OPERATOR_NODE_NAME;

        case NodeTypes.DROP_TRIGGER_NODE:
            return NodeNames.DROP_TRIGGER_NODE_NAME;

        case NodeTypes.BINARY_BIT_OPERATOR_NODE:
            return NodeNames.BINARY_BIT_OPERATOR_NODE_NAME;

        case NodeTypes.TINYINT_CONSTANT_NODE:
        case NodeTypes.SMALLINT_CONSTANT_NODE:
        case NodeTypes.INT_CONSTANT_NODE:
        case NodeTypes.LONGINT_CONSTANT_NODE:
        case NodeTypes.DECIMAL_CONSTANT_NODE:
        case NodeTypes.DOUBLE_CONSTANT_NODE:
        case NodeTypes.FLOAT_CONSTANT_NODE:
            return NodeNames.NUMERIC_CONSTANT_NODE_NAME;

        case NodeTypes.USERTYPE_CONSTANT_NODE:
            return NodeNames.USERTYPE_CONSTANT_NODE_NAME;

        case NodeTypes.PREDICATE:
            return NodeNames.PREDICATE_NAME;

        case NodeTypes.RESULT_COLUMN:
            return NodeNames.RESULT_COLUMN_NAME;

        case NodeTypes.SET_ROLE_NODE:
            return NodeNames.SET_ROLE_NODE_NAME;

        case NodeTypes.SET_SCHEMA_NODE:
            return NodeNames.SET_SCHEMA_NODE_NAME;

        case NodeTypes.SIMPLE_STRING_OPERATOR_NODE:
            return NodeNames.SIMPLE_STRING_OPERATOR_NODE_NAME;

        case NodeTypes.STATIC_CLASS_FIELD_REFERENCE_NODE:
            return NodeNames.STATIC_CLASS_FIELD_REFERENCE_NODE_NAME;

        case NodeTypes.STATIC_METHOD_CALL_NODE:
            return NodeNames.STATIC_METHOD_CALL_NODE_NAME;

        case NodeTypes.EXTRACT_OPERATOR_NODE:
            return NodeNames.EXTRACT_OPERATOR_NODE_NAME;

        case NodeTypes.PARAMETER_NODE:
            return NodeNames.PARAMETER_NODE_NAME;

        case NodeTypes.DROP_SCHEMA_NODE:
            return NodeNames.DROP_SCHEMA_NODE_NAME;

        case NodeTypes.DROP_ROLE_NODE:
            return NodeNames.DROP_ROLE_NODE_NAME;

        case NodeTypes.DROP_TABLE_NODE:
            return NodeNames.DROP_TABLE_NODE_NAME;

        case NodeTypes.DROP_VIEW_NODE:
            return NodeNames.DROP_VIEW_NODE_NAME;

        case NodeTypes.DROP_GROUP_NODE:
            return NodeNames.DROP_GROUP_NODE_NAME;
            
        case NodeTypes.GROUP_CONCAT_NODE:
            return NodeNames.GROUP_CONCAT_NODE_NAME;

        case NodeTypes.SUBQUERY_NODE:
            return NodeNames.SUBQUERY_NODE_NAME;

        case NodeTypes.BASE_COLUMN_NODE:
            return NodeNames.BASE_COLUMN_NODE_NAME;

        case NodeTypes.CALL_STATEMENT_NODE:
            return NodeNames.CALL_STATEMENT_NODE_NAME;

        case NodeTypes.MODIFY_COLUMN_DEFAULT_NODE:
        case NodeTypes.MODIFY_COLUMN_TYPE_NODE:
        case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NODE:
        case NodeTypes.MODIFY_COLUMN_CONSTRAINT_NOT_NULL_NODE:
        case NodeTypes.DROP_COLUMN_NODE:
            return NodeNames.MODIFY_COLUMN_NODE_NAME;

        case NodeTypes.NON_STATIC_METHOD_CALL_NODE:
            return NodeNames.NON_STATIC_METHOD_CALL_NODE_NAME;

        case NodeTypes.CURRENT_OF_NODE:
            return NodeNames.CURRENT_OF_NODE_NAME;

        case NodeTypes.DEFAULT_NODE:
            return NodeNames.DEFAULT_NODE_NAME;

        case NodeTypes.DELETE_NODE:
            return NodeNames.DELETE_NODE_NAME;

        case NodeTypes.UPDATE_NODE:
            return NodeNames.UPDATE_NODE_NAME;

        case NodeTypes.ORDER_BY_COLUMN:
            return NodeNames.ORDER_BY_COLUMN_NAME;

        case NodeTypes.ROW_RESULT_SET_NODE:
            return NodeNames.ROW_RESULT_SET_NODE_NAME;

        case NodeTypes.VIRTUAL_COLUMN_NODE:
            return NodeNames.VIRTUAL_COLUMN_NODE_NAME;

        case NodeTypes.CURRENT_DATETIME_OPERATOR_NODE:
            return NodeNames.CURRENT_DATETIME_OPERATOR_NODE_NAME;

        case NodeTypes.USER_NODE:
        case NodeTypes.CURRENT_USER_NODE:
        case NodeTypes.SESSION_USER_NODE:
        case NodeTypes.SYSTEM_USER_NODE:
        case NodeTypes.CURRENT_ISOLATION_NODE:
        case NodeTypes.IDENTITY_VAL_NODE:
        case NodeTypes.CURRENT_SCHEMA_NODE:
        case NodeTypes.CURRENT_ROLE_NODE:
            return NodeNames.SPECIAL_FUNCTION_NODE_NAME;

        case NodeTypes.IS_NODE:
            return NodeNames.IS_NODE_NAME;

        case NodeTypes.LOCK_TABLE_NODE:
            return NodeNames.LOCK_TABLE_NODE_NAME;

        case NodeTypes.ALTER_TABLE_NODE:
            return NodeNames.ALTER_TABLE_NODE_NAME;

        case NodeTypes.ALTER_SERVER_NODE:
            return NodeNames.ALTER_SERVER_NODE_NAME;
            
        case NodeTypes.AGGREGATE_NODE:
            return NodeNames.AGGREGATE_NODE_NAME;

        case NodeTypes.COLUMN_DEFINITION_NODE:
            return NodeNames.COLUMN_DEFINITION_NODE_NAME;

        case NodeTypes.FK_CONSTRAINT_DEFINITION_NODE:
            return NodeNames.FK_CONSTRAINT_DEFINITION_NODE_NAME;

        case NodeTypes.FROM_VTI:
            return NodeNames.FROM_VTI_NAME;

        case NodeTypes.MATERIALIZE_RESULT_SET_NODE:
            return NodeNames.MATERIALIZE_RESULT_SET_NODE_NAME;

        case NodeTypes.NORMALIZE_RESULT_SET_NODE:
            return NodeNames.NORMALIZE_RESULT_SET_NODE_NAME;

        case NodeTypes.SCROLL_INSENSITIVE_RESULT_SET_NODE:
            return NodeNames.SCROLL_INSENSITIVE_RESULT_SET_NODE_NAME;

        case NodeTypes.ORDER_BY_NODE:
            return NodeNames.ORDER_BY_NODE_NAME;

        case NodeTypes.DISTINCT_NODE:
            return NodeNames.DISTINCT_NODE_NAME;

        case NodeTypes.LOCATE_FUNCTION_NODE:
        case NodeTypes.SUBSTRING_OPERATOR_NODE:
        case NodeTypes.TIMESTAMP_ADD_FN_NODE:
        case NodeTypes.TIMESTAMP_DIFF_FN_NODE:
            return NodeNames.TERNARY_OPERATOR_NODE_NAME;

        case NodeTypes.SELECT_NODE:
            return NodeNames.SELECT_NODE_NAME;

        case NodeTypes.CREATE_VIEW_NODE:
            return NodeNames.CREATE_VIEW_NODE_NAME;

        case NodeTypes.CONSTRAINT_DEFINITION_NODE:
            return NodeNames.CONSTRAINT_DEFINITION_NODE_NAME;

        case NodeTypes.NEW_INVOCATION_NODE:
            return NodeNames.NEW_INVOCATION_NODE_NAME;

        case NodeTypes.CREATE_ROLE_NODE:
            return NodeNames.CREATE_ROLE_NODE_NAME;

        case NodeTypes.CREATE_SCHEMA_NODE:
            return NodeNames.CREATE_SCHEMA_NODE_NAME;

        case NodeTypes.FROM_BASE_TABLE:
            return NodeNames.FROM_BASE_TABLE_NAME;

        case NodeTypes.FROM_SUBQUERY:
            return NodeNames.FROM_SUBQUERY_NAME;

        case NodeTypes.GROUP_BY_NODE:
            return NodeNames.GROUP_BY_NODE_NAME;

        case NodeTypes.INSERT_NODE:
            return NodeNames.INSERT_NODE_NAME;

        case NodeTypes.JOIN_NODE:
            return NodeNames.JOIN_NODE_NAME;

        case NodeTypes.CREATE_TABLE_NODE:
            return NodeNames.CREATE_TABLE_NODE_NAME;

        case NodeTypes.RENAME_NODE:
            return NodeNames.RENAME_NODE_NAME;

        case NodeTypes.UNION_NODE:
            return NodeNames.UNION_NODE_NAME;

        case NodeTypes.INTERSECT_OR_EXCEPT_NODE:
            return NodeNames.INTERSECT_OR_EXCEPT_NODE_NAME;

        case NodeTypes.CREATE_TRIGGER_NODE:
            return NodeNames.CREATE_TRIGGER_NODE_NAME;

        case NodeTypes.HALF_OUTER_JOIN_NODE:
            return NodeNames.HALF_OUTER_JOIN_NODE_NAME;

        case NodeTypes.FULL_OUTER_JOIN_NODE:
            return NodeNames.FULL_OUTER_JOIN_NODE_NAME;

        case NodeTypes.EXPLICIT_COLLATE_NODE:
            return NodeNames.EXPLICIT_COLLATE_NODE_NAME;

        case NodeTypes.CREATE_INDEX_NODE:
            return NodeNames.CREATE_INDEX_NODE_NAME;

        case NodeTypes.CURSOR_NODE:
            return NodeNames.CURSOR_NODE_NAME;

        case NodeTypes.HASH_TABLE_NODE:
            return NodeNames.HASH_TABLE_NODE_NAME;

        case NodeTypes.INDEX_TO_BASE_ROW_NODE:
            return NodeNames.INDEX_TO_BASE_ROW_NODE_NAME;

        case NodeTypes.CREATE_ALIAS_NODE:
            return NodeNames.CREATE_ALIAS_NODE_NAME;

        case NodeTypes.PROJECT_RESTRICT_NODE:
            return NodeNames.PROJECT_RESTRICT_NODE_NAME;

        case NodeTypes.SQL_BOOLEAN_CONSTANT_NODE:
            return NodeNames.SQL_BOOLEAN_CONSTANT_NODE_NAME;

        case NodeTypes.DROP_ALIAS_NODE:
            return NodeNames.DROP_ALIAS_NODE_NAME;

        case NodeTypes.TEST_CONSTRAINT_NODE:
            return NodeNames.TEST_CONSTRAINT_NODE_NAME;

        case NodeTypes.ABSOLUTE_OPERATOR_NODE:
        case NodeTypes.SQRT_OPERATOR_NODE:
        case NodeTypes.UNARY_PLUS_OPERATOR_NODE:
        case NodeTypes.UNARY_MINUS_OPERATOR_NODE:
            return NodeNames.UNARY_ARITHMETIC_OPERATOR_NODE_NAME;

        case NodeTypes.TRANSACTION_CONTROL_NODE:
            return NodeNames.TRANSACTION_CONTROL_NODE_NAME;

        case NodeTypes.SAVEPOINT_NODE:
            return NodeNames.SAVEPOINT_NODE_NAME;

        case NodeTypes.UNARY_DATE_TIMESTAMP_OPERATOR_NODE:
            return NodeNames.UNARY_DATE_TIMESTAMP_OPERATOR_NODE_NAME;

        case NodeTypes.TIMESTAMP_OPERATOR_NODE:
            return NodeNames.TIMESTAMP_OPERATOR_NODE_NAME;

        case NodeTypes.OCTET_LENGTH_OPERATOR_NODE:
            return NodeNames.OCTET_LENGTH_OPERATOR_NODE_NAME;

        case NodeTypes.XML_PARSE_OPERATOR_NODE:
        case NodeTypes.XML_SERIALIZE_OPERATOR_NODE:
            return NodeNames.XML_UNARY_OPERATOR_NODE_NAME;

        case NodeTypes.XML_EXISTS_OPERATOR_NODE:
        case NodeTypes.XML_QUERY_OPERATOR_NODE:
            return NodeNames.XML_BINARY_OPERATOR_NODE_NAME;

        case NodeTypes.GRANT_NODE:
            return NodeNames.GRANT_NODE_NAME;
        case NodeTypes.REVOKE_NODE:
            return NodeNames.REVOKE_NODE_NAME;

        case NodeTypes.GRANT_ROLE_NODE:
            return NodeNames.GRANT_ROLE_NODE_NAME;

        case NodeTypes.REVOKE_ROLE_NODE:
            return NodeNames.REVOKE_ROLE_NODE_NAME;

        case NodeTypes.PRIVILEGE_NODE:
            return NodeNames.PRIVILEGE_NAME;

        case NodeTypes.TABLE_PRIVILEGES_NODE:
            return NodeNames.TABLE_PRIVILEGES_NAME;

        case NodeTypes.AGGREGATE_WINDOW_FUNCTION_NODE:
            return NodeNames.AGGREGATE_WINDOW_FUNCTION_NAME;

        case NodeTypes.ROW_NUMBER_FUNCTION_NODE:
            return NodeNames.ROW_NUMBER_FUNCTION_NAME;

        case NodeTypes.WINDOW_DEFINITION_NODE:
            return NodeNames.WINDOW_DEFINITION_NAME;

        case NodeTypes.WINDOW_REFERENCE_NODE:
            return NodeNames.WINDOW_REFERENCE_NAME;

        case NodeTypes.WINDOW_RESULTSET_NODE:
            return NodeNames.WINDOW_RESULTSET_NODE_NAME;

        case NodeTypes.GENERATION_CLAUSE_NODE:
            return NodeNames.GENERATION_CLAUSE_NODE_NAME;
 
        case NodeTypes.ROW_COUNT_NODE:
            return NodeNames.ROW_COUNT_NODE_NAME;

        case NodeTypes.CREATE_SEQUENCE_NODE:
            return NodeNames.CREATE_SEQUENCE_NODE_NAME;

        case NodeTypes.DROP_SEQUENCE_NODE:
            return NodeNames.DROP_SEQUENCE_NODE_NAME;

        case NodeTypes.NEXT_SEQUENCE_NODE:
            return NodeNames.NEXT_SEQUENCE_NODE_NAME;

        case NodeTypes.CURRENT_SEQUENCE_NODE:
            return NodeNames.CURRENT_SEQUENCE_NODE_NAME;

        case NodeTypes.EXPLAIN_STATEMENT_NODE:
            return NodeNames.EXPLAIN_STATEMENT_NODE_NAME;

        case NodeTypes.COPY_STATEMENT_NODE:
            return NodeNames.COPY_STATEMENT_NODE_NAME;

        case NodeTypes.INDEX_COLUMN:
            return NodeNames.INDEX_COLUMN_NAME;
        
        case NodeTypes.INDEX_COLUMN_LIST:
            return NodeNames.INDEX_COLUMN_LIST_NAME;

        case NodeTypes.INDEX_HINT_NODE:
            return NodeNames.INDEX_HINT_NODE_NAME;
        
        case NodeTypes.INDEX_HINT_LIST:
            return NodeNames.INDEX_HINT_LIST_NAME;
        
        case NodeTypes.RIGHT_FN_NODE:
        case NodeTypes.LEFT_FN_NODE:
            return NodeNames.LEFT_RIGHT_FUNC_OPERATOR_NODE_NAME;

        case NodeTypes.ROW_CTOR_NODE:
            return NodeNames.ROW_CTOR_NODE_NAME;

        case NodeTypes.ROWS_RESULT_SET_NODE:
            return NodeNames.ROWS_RESULT_SET_NODE_NAME;
            
        case NodeTypes.AT_DROP_INDEX_NODE:
            return NodeNames.AT_DROP_INDEX_NODE_NAME;
            
        case NodeTypes.AT_ADD_INDEX_NODE:
            return NodeNames.AT_ADD_INDEX_NODE_NAME;

        case NodeTypes.INDEX_CONSTRAINT_NODE:
            return NodeNames.INDEX_CONSTRAINT_NAME;

        case NodeTypes.AT_RENAME_NODE:
            return NodeNames.AT_RENAME_NODE_NAME;
            
        case NodeTypes.AT_RENAME_COLUMN_NODE:
            return NodeNames.AT_RENAME_COLUMN_NODE_NAME;

        case NodeTypes.DECLARE_STATEMENT_NODE:
            return NodeNames.DECLARE_STATEMENT_NODE_NAME;

        case NodeTypes.FETCH_STATEMENT_NODE:
            return NodeNames.FETCH_STATEMENT_NODE_NAME;

        case NodeTypes.CLOSE_STATEMENT_NODE:
            return NodeNames.CLOSE_STATEMENT_NODE_NAME;

        case NodeTypes.PREPARE_STATEMENT_NODE:
            return NodeNames.PREPARE_STATEMENT_NODE_NAME;

        case NodeTypes.EXECUTE_STATEMENT_NODE:
            return NodeNames.EXECUTE_STATEMENT_NODE_NAME;

        case NodeTypes.DEALLOCATE_STATEMENT_NODE:
            return NodeNames.DEALLOCATE_STATEMENT_NODE_NAME;

        case NodeTypes.SIMPLE_CASE_NODE:
            return NodeNames.SIMPLE_CASE_NODE_NAME;

        case NodeTypes.PARTITION_BY_LIST:
            return NodeNames.PARTITION_BY_LIST_NAME;

        case NodeTypes.PARTITION_BY_COLUMN:
            return NodeNames.PARTITION_BY_COLUMN_NAME;

        case NodeTypes.NESTED_COLUMN_REFERENCE:
            return NodeNames.NESTED_COLUMN_REFERENCE_NAME;

        case NodeTypes.SYSTEM_COLUMN_REFERENCE:
            return NodeNames.SYSTEM_COLUMN_REFERENCE_NAME;
        case NodeTypes.GENERIC_PROPERTIES:
            return NodeNames.GENERIC_PROPERTIES_NAME;
        case NodeTypes.CREATE_ANALYZER_NODE:
            return NodeNames.CREATE_ANALYZER_NODE_NAME;
        case NodeTypes.ANALYZER_ELEMENTS:
            return NodeNames.ANALYZER_ELEMENTS_NAME;
        case NodeTypes.TOKEN_FILTER_LIST:
            return NodeNames.TOKEN_FILTER_LIST_NAME;
        case NodeTypes.CHAR_FILTER_LIST:
            return NodeNames.CHAR_FILTER_LIST_NAME;
        case NodeTypes.NAMED_NODE_WITH_OPTIONAL_PROPERTIES:
            return NodeNames.NAMED_NODE_WITH_OPTIONAL_PROPERTIES_NAME;
        default:
            throw new StandardException("Not implemented");
        }
    }

}

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

   Derby - Class org.apache.derby.impl.sql.compile.C_NodeNames

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
 * This is the set of constants used to identify the classes
 * that are used in NodeFactoryImpl.
 *
 * This class is not shipped. The names are used in
 * NodeFactoryImpl, mapped from int NodeTypes and used in
 * Class.forName calls.
 *
 * WARNING: WHEN ADDING NODE TYPES HERE, YOU MUST ALSO ADD
 * THEM TO tools/jar/DBMSnodes.properties
 *
 */

public interface NodeNames
{

    // The names are in alphabetic order.

    static final String AGGREGATE_NODE_NAME = "org.cratedb.sql.parser.parser.AggregateNode";

    static final String AGGREGATE_WINDOW_FUNCTION_NAME = "org.cratedb.sql.parser.parser.AggregateWindowFunctionNode";

    static final String ALL_RESULT_COLUMN_NAME = "org.cratedb.sql.parser.parser.AllResultColumn";

    static final String ALTER_SERVER_NODE_NAME = "org.cratedb.sql.parser.parser.AlterServerNode";
    
    static final String ALTER_TABLE_NODE_NAME = "org.cratedb.sql.parser.parser.AlterTableNode";

    static final String ANALYZER_ELEMENTS_NAME = "org.cratedb.sql.parser.parser.AnalyzerElements";

    static final String AT_DROP_INDEX_NODE_NAME = "org.cratedb.sql.parser.parser.AlterDropIndexNode";
    
    static final String AT_ADD_INDEX_NODE_NAME = "org.cratedb.sql.parser.parser.AlterAddIndexNode";

    static final String AT_RENAME_NODE_NAME = "org.cratedb.sql.parser.parser.AlterTableRenameNode";

    static final String AT_RENAME_COLUMN_NODE_NAME = "org.cratedb.sql.parser.parser.AlterTableRenameColumnNode";
    
    static final String AND_NODE_NAME = "org.cratedb.sql.parser.parser.AndNode";

    static final String BASE_COLUMN_NODE_NAME = "org.cratedb.sql.parser.parser.BaseColumnNode";

    static final String BETWEEN_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.BetweenOperatorNode";

    static final String BINARY_ARITHMETIC_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.BinaryArithmeticOperatorNode";

    static final String BINARY_BIT_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.BinaryBitOperatorNode";

    static final String BINARY_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.BinaryOperatorNode";

    static final String BINARY_RELATIONAL_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.BinaryRelationalOperatorNode";

    static final String LEFT_RIGHT_FUNC_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.LeftRightFuncOperatorNode";
    
    static final String ROW_CTOR_NODE_NAME = "org.cratedb.sql.parser.parser.RowConstructorNode";

    static final String BIT_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.BitConstantNode";

    static final String BOOLEAN_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.BooleanConstantNode";

    static final String CALL_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.CallStatementNode";

    static final String CAST_NODE_NAME = "org.cratedb.sql.parser.parser.CastNode";

    static final String CHAR_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.CharConstantNode";

    static final String CHAR_FILTER_LIST_NAME = "org.cratedb.sql.parser.parser.CharFilterList";

    static final String CHAR_FILTER_NODE_NAME = "org.cratedb.sql.parser.parser.CharFilterNode";

    static final String CLOSE_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.CloseStatementNode";

    static final String COALESCE_FUNCTION_NODE_NAME = "org.cratedb.sql.parser.parser.CoalesceFunctionNode";

    static final String COLUMN_DEFINITION_NODE_NAME = "org.cratedb.sql.parser.parser.ColumnDefinitionNode";

    static final String COLUMN_REFERENCE_NAME = "org.cratedb.sql.parser.parser.ColumnReference";

    static final String CONCATENATION_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.ConcatenationOperatorNode";

    static final String CONDITIONAL_NODE_NAME = "org.cratedb.sql.parser.parser.ConditionalNode";

    static final String CONSTRAINT_DEFINITION_NODE_NAME = "org.cratedb.sql.parser.parser.ConstraintDefinitionNode";

    static final String COPY_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.CopyStatementNode";

    static final String CREATE_ALIAS_NODE_NAME = "org.cratedb.sql.parser.parser.CreateAliasNode";

    static final String CREATE_ANALYZER_NODE_NAME = "org.cratedb.sql.parser.parser.CreateAnalyzerNode";

    static final String CREATE_INDEX_NODE_NAME = "org.cratedb.sql.parser.parser.CreateIndexNode";

    static final String CREATE_ROLE_NODE_NAME = "org.cratedb.sql.parser.parser.CreateRoleNode";

    static final String CREATE_SCHEMA_NODE_NAME = "org.cratedb.sql.parser.parser.CreateSchemaNode";

    static final String CREATE_SEQUENCE_NODE_NAME = "org.cratedb.sql.parser.parser.CreateSequenceNode";

    static final String CREATE_TABLE_NODE_NAME = "org.cratedb.sql.parser.parser.CreateTableNode";

    static final String CREATE_TRIGGER_NODE_NAME = "org.cratedb.sql.parser.parser.CreateTriggerNode";

    static final String CREATE_VIEW_NODE_NAME = "org.cratedb.sql.parser.parser.CreateViewNode";

    static final String CURRENT_DATETIME_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.CurrentDatetimeOperatorNode";

    static final String CURRENT_OF_NODE_NAME = "org.cratedb.sql.parser.parser.CurrentOfNode";

    static final String CURRENT_ROW_LOCATION_NODE_NAME = "org.cratedb.sql.parser.parser.CurrentRowLocationNode";

    static final String CURSOR_NODE_NAME = "org.cratedb.sql.parser.parser.CursorNode";

    static final String OCTET_LENGTH_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.OctetLengthOperatorNode";

    static final String DEALLOCATE_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.DeallocateStatementNode";

    static final String DECLARE_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.DeclareStatementNode";

    static final String DEFAULT_NODE_NAME = "org.cratedb.sql.parser.parser.DefaultNode";

    static final String DELETE_NODE_NAME = "org.cratedb.sql.parser.parser.DeleteNode";

    static final String DISTINCT_NODE_NAME = "org.cratedb.sql.parser.parser.DistinctNode";

    static final String DML_MOD_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.DMLModStatementNode";

    static final String DROP_ALIAS_NODE_NAME = "org.cratedb.sql.parser.parser.DropAliasNode";

    static final String DROP_INDEX_NODE_NAME = "org.cratedb.sql.parser.parser.DropIndexNode";
    
    static final String DROP_GROUP_NODE_NAME = "org.cratedb.sql.parser.parser.DropGroupNode";

    static final String DROP_ROLE_NODE_NAME = "org.cratedb.sql.parser.parser.DropRoleNode";

    static final String DROP_SCHEMA_NODE_NAME = "org.cratedb.sql.parser.parser.DropSchemaNode";

    static final String DROP_SEQUENCE_NODE_NAME = "org.cratedb.sql.parser.parser.DropSequenceNode";

    static final String DROP_TABLE_NODE_NAME = "org.cratedb.sql.parser.parser.DropTableNode";

    static final String DROP_TRIGGER_NODE_NAME = "org.cratedb.sql.parser.parser.DropTriggerNode";

    static final String DROP_VIEW_NODE_NAME = "org.cratedb.sql.parser.parser.DropViewNode";

    static final String EXECUTE_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.ExecuteStatementNode";

    static final String EXPLAIN_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.ExplainStatementNode";

    static final String EXPLICIT_COLLATE_NODE_NAME = "org.cratedb.sql.parser.parser.ExplicitCollateNode";

    static final String EXTRACT_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.ExtractOperatorNode";

    static final String FETCH_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.FetchStatementNode";

    static final String FK_CONSTRAINT_DEFINITION_NODE_NAME = "org.cratedb.sql.parser.parser.FKConstraintDefinitionNode";

    static final String FROM_BASE_TABLE_NAME = "org.cratedb.sql.parser.parser.FromBaseTable";

    static final String FROM_LIST_NAME = "org.cratedb.sql.parser.parser.FromList";

    static final String FROM_SUBQUERY_NAME = "org.cratedb.sql.parser.parser.FromSubquery";

    static final String FROM_VTI_NAME = "org.cratedb.sql.parser.parser.FromVTI";

    static final String FULL_OUTER_JOIN_NODE_NAME = "org.cratedb.sql.parser.parser.FullOuterJoinNode";

    static final String GENERATION_CLAUSE_NODE_NAME = "org.cratedb.sql.parser.parser.GenerationClauseNode";

    static final String GENERIC_PROPERTIES_NAME = "org.cratedb.sql.parser.parser.GenericProperties";

    static final String GET_CURRENT_CONNECTION_NODE_NAME = "org.cratedb.sql.parser.parser.GetCurrentConnectionNode";

    static final String GRANT_NODE_NAME = "org.cratedb.sql.parser.parser.GrantNode";

    static final String GRANT_ROLE_NODE_NAME = "org.cratedb.sql.parser.parser.GrantRoleNode";

    static final String GROUP_BY_COLUMN_NAME = "org.cratedb.sql.parser.parser.GroupByColumn";

    static final String GROUP_BY_LIST_NAME = "org.cratedb.sql.parser.parser.GroupByList";

    static final String GROUP_BY_NODE_NAME = "org.cratedb.sql.parser.parser.GroupByNode";

    static final String GROUP_CONCAT_NODE_NAME = "org.cratedb.sql.parser.parser.GroupConcatNode";

    static final String HALF_OUTER_JOIN_NODE_NAME = "org.cratedb.sql.parser.parser.HalfOuterJoinNode";

    static final String HASH_TABLE_NODE_NAME = "org.cratedb.sql.parser.parser.HashTableNode";

    static final String INDEX_COLUMN_NAME = "org.cratedb.sql.parser.parser.IndexColumn";

    static final String INDEX_COLUMN_LIST_NAME = "org.cratedb.sql.parser.parser.IndexColumnList";

    static final String INDEX_CONSTRAINT_NAME = "org.cratedb.sql.parser.parser.IndexConstraintDefinitionNode";

    static final String INDEX_HINT_NODE_NAME = "org.cratedb.sql.parser.parser.IndexHintNode";

    static final String INDEX_HINT_LIST_NAME = "org.cratedb.sql.parser.parser.IndexHintList";

    static final String INDEX_TO_BASE_ROW_NODE_NAME = "org.cratedb.sql.parser.parser.IndexToBaseRowNode";

    static final String INSERT_NODE_NAME = "org.cratedb.sql.parser.parser.InsertNode";

    static final String INTERSECT_OR_EXCEPT_NODE_NAME = "org.cratedb.sql.parser.parser.IntersectOrExceptNode";

    static final String IN_LIST_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.InListOperatorNode";

    static final String IS_NODE_NAME = "org.cratedb.sql.parser.parser.IsNode";

    static final String IS_NULL_NODE_NAME = "org.cratedb.sql.parser.parser.IsNullNode";

    static final String JAVA_TO_SQL_VALUE_NODE_NAME = "org.cratedb.sql.parser.parser.JavaToSQLValueNode";

    static final String JOIN_NODE_NAME = "org.cratedb.sql.parser.parser.JoinNode";

    static final String LENGTH_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.LengthOperatorNode";

    static final String LIKE_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.LikeEscapeOperatorNode";

    static final String LOCK_TABLE_NODE_NAME = "org.cratedb.sql.parser.parser.LockTableNode";

    static final String MATERIALIZE_RESULT_SET_NODE_NAME = "org.cratedb.sql.parser.parser.MaterializeResultSetNode";

    static final String MODIFY_COLUMN_NODE_NAME = "org.cratedb.sql.parser.parser.ModifyColumnNode";

    static final String NAMED_NODE_WITH_OPTIONAL_PROPERTIES_NAME = "org.cratedb.sql.parser.parser.NamedNodeWithOptionalProperties";

    static final String NESTED_COLUMN_REFERENCE_NAME = "org.cratedb.sql.parser.parser.NestedColumnReference";

    static final String NEW_INVOCATION_NODE_NAME = "org.cratedb.sql.parser.parser.NewInvocationNode";

    static final String NEXT_SEQUENCE_NODE_NAME = "org.cratedb.sql.parser.parser.NextSequenceNode";

    static final String CURRENT_SEQUENCE_NODE_NAME = "org.cratedb.sql.parser.parser.CurrentSequenceNode";

    static final String NON_STATIC_METHOD_CALL_NODE_NAME = "org.cratedb.sql.parser.parser.NonStaticMethodCallNode";

    static final String NOP_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.NOPStatementNode";

    static final String NORMALIZE_RESULT_SET_NODE_NAME = "org.cratedb.sql.parser.parser.NormalizeResultSetNode";

    static final String NOT_NODE_NAME = "org.cratedb.sql.parser.parser.NotNode";

    static final String NUMERIC_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.NumericConstantNode";

    static final String OR_NODE_NAME = "org.cratedb.sql.parser.parser.OrNode";

    static final String ORDER_BY_COLUMN_NAME = "org.cratedb.sql.parser.parser.OrderByColumn";

    static final String ORDER_BY_LIST_NAME = "org.cratedb.sql.parser.parser.OrderByList";

    static final String ORDER_BY_NODE_NAME = "org.cratedb.sql.parser.parser.OrderByNode";

    static final String PARAMETER_NODE_NAME = "org.cratedb.sql.parser.parser.ParameterNode";

    static final String PARTITION_BY_COLUMN_NAME = "org.cratedb.sql.parser.parser.PartitionByColumn";

    static final String PARTITION_BY_LIST_NAME = "org.cratedb.sql.parser.parser.PartitionByList";

    static final String PREDICATE_LIST_NAME = "org.cratedb.sql.parser.parser.PredicateList";

    static final String PREDICATE_NAME = "org.cratedb.sql.parser.parser.Predicate";

    static final String PREPARE_STATEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.PrepareStatementNode";

    static final String PRIVILEGE_NAME = "org.cratedb.sql.parser.parser.PrivilegeNode";

    static final String PROJECT_RESTRICT_NODE_NAME = "org.cratedb.sql.parser.parser.ProjectRestrictNode";

    static final String RENAME_NODE_NAME = "org.cratedb.sql.parser.parser.RenameNode";

    static final String RESULT_COLUMN_LIST_NAME = "org.cratedb.sql.parser.parser.ResultColumnList";

    static final String RESULT_COLUMN_NAME = "org.cratedb.sql.parser.parser.ResultColumn";

    static final String REVOKE_NODE_NAME = "org.cratedb.sql.parser.parser.RevokeNode";

    static final String REVOKE_ROLE_NODE_NAME = "org.cratedb.sql.parser.parser.RevokeRoleNode";

    static final String ROW_COUNT_NODE_NAME = "org.cratedb.sql.parser.parser.RowCountNode";

    static final String ROW_NUMBER_FUNCTION_NAME = "org.cratedb.sql.parser.parser.RowNumberFunctionNode";

    static final String ROW_RESULT_SET_NODE_NAME = "org.cratedb.sql.parser.parser.RowResultSetNode";

    static final String ROWS_RESULT_SET_NODE_NAME = "org.cratedb.sql.parser.parser.RowsResultSetNode";

    static final String SAVEPOINT_NODE_NAME = "org.cratedb.sql.parser.parser.SavepointNode";

    static final String SCROLL_INSENSITIVE_RESULT_SET_NODE_NAME = "org.cratedb.sql.parser.parser.ScrollInsensitiveResultSetNode";

    static final String SELECT_NODE_NAME = "org.cratedb.sql.parser.parser.SelectNode";

    static final String SET_CONFIGURATION_NODE_NAME = "org.cratedb.sql.parser.parser.SetConfigurationNode";

    static final String SET_ROLE_NODE_NAME = "org.cratedb.sql.parser.parser.SetRoleNode";

    static final String SET_SCHEMA_NODE_NAME = "org.cratedb.sql.parser.parser.SetSchemaNode";

    static final String SET_TRANSACTION_ACCESS_NODE_NAME = "org.cratedb.sql.parser.parser.SetTransactionAccessNode";

    static final String SET_TRANSACTION_ISOLATION_NODE_NAME = "org.cratedb.sql.parser.parser.SetTransactionIsolationNode";

    static final String SIMPLE_CASE_NODE_NAME = "org.cratedb.sql.parser.parser.SimpleCaseNode";

    static final String SIMPLE_STRING_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.SimpleStringOperatorNode";

    static final String SPECIAL_FUNCTION_NODE_NAME = "org.cratedb.sql.parser.parser.SpecialFunctionNode";

    static final String SQL_BOOLEAN_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.SQLBooleanConstantNode";

    static final String SQL_TO_JAVA_VALUE_NODE_NAME = "org.cratedb.sql.parser.parser.SQLToJavaValueNode";

    static final String STATIC_CLASS_FIELD_REFERENCE_NODE_NAME = "org.cratedb.sql.parser.parser.StaticClassFieldReferenceNode";

    static final String STATIC_METHOD_CALL_NODE_NAME = "org.cratedb.sql.parser.parser.StaticMethodCallNode";

    static final String SUBQUERY_LIST_NAME = "org.cratedb.sql.parser.parser.SubqueryList";

    static final String SUBQUERY_NODE_NAME = "org.cratedb.sql.parser.parser.SubqueryNode";

    static final String SYSTEM_COLUMN_REFERENCE_NAME = "org.cratedb.sql.parser.parser.SystemColumnReference";

    static final String TABLE_ELEMENT_LIST_NAME = "org.cratedb.sql.parser.parser.TableElementList";

    static final String TABLE_ELEMENT_NODE_NAME = "org.cratedb.sql.parser.parser.TableElementNode";

    static final String TABLE_NAME_NAME = "org.cratedb.sql.parser.parser.TableName";

    static final String TABLE_PRIVILEGES_NAME = "org.cratedb.sql.parser.parser.TablePrivilegesNode";

    static final String TERNARY_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.TernaryOperatorNode";

    static final String TEST_CONSTRAINT_NODE_NAME = "org.cratedb.sql.parser.parser.TestConstraintNode";

    static final String TIMESTAMP_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.TimestampOperatorNode";

    static final String TOKEN_FILTER_LIST_NAME = "org.cratedb.sql.parser.parser.TokenFilterList";

    static final String TOKEN_FILTER_NODE_NAME = "org.cratedb.sql.parser.parser.TokenFilterNode";

    static final String TRANSACTION_CONTROL_NODE_NAME = "org.cratedb.sql.parser.parser.TransactionControlNode";

    static final String TRIM_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.TrimOperatorNode";
    
    static final String UNARY_ARITHMETIC_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.UnaryArithmeticOperatorNode";

    static final String UNARY_BIT_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.UnaryBitOperatorNode";

    static final String UNARY_DATE_TIMESTAMP_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.UnaryDateTimestampOperatorNode";

    static final String UNARY_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.UnaryOperatorNode";

    static final String UNION_NODE_NAME = "org.cratedb.sql.parser.parser.UnionNode";

    static final String UNTYPED_NULL_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.UntypedNullConstantNode";

    static final String UPDATE_NODE_NAME = "org.cratedb.sql.parser.parser.UpdateNode";

    static final String USERTYPE_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.UserTypeConstantNode";

    static final String VALUE_NODE_LIST_NAME = "org.cratedb.sql.parser.parser.ValueNodeList";

    static final String VARBIT_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.VarbitConstantNode";

    static final String VIRTUAL_COLUMN_NODE_NAME = "org.cratedb.sql.parser.parser.VirtualColumnNode";

    static final String WINDOW_DEFINITION_NAME = "org.cratedb.sql.parser.parser.WindowDefinitionNode";

    static final String WINDOW_REFERENCE_NAME = "org.cratedb.sql.parser.parser.WindowReferenceNode";

    static final String WINDOW_RESULTSET_NODE_NAME = "org.cratedb.sql.parser.parser.WindowResultSetNode";

    static final String XML_BINARY_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.XMLBinaryOperatorNode";

    static final String XML_CONSTANT_NODE_NAME = "org.cratedb.sql.parser.parser.XMLConstantNode";

    static final String XML_UNARY_OPERATOR_NODE_NAME = "org.cratedb.sql.parser.parser.XMLUnaryOperatorNode";

    // The names are in alphabetic order.

}

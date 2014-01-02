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

package org.cratedb.sql.parser.unparser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;

import java.util.Map;

public class NodeToString
{
    public NodeToString() {
    }

    public String toString(QueryTreeNode node) throws StandardException {
        switch (node.getNodeType()) {
        case CREATE_TABLE_NODE:
            return createTableNode((CreateTableNode)node);
        case CREATE_VIEW_NODE:
            return createViewNode((CreateViewNode)node);
        case CREATE_ANALYZER_NODE:
            return createAnalyzerNode((CreateAnalyzerNode)node);
        case NAMED_NODE_WITH_OPTIONAL_PROPERTIES:
            return namedNodeWithOptionalProperties((NamedNodeWithOptionalProperties)node);
        case DROP_TABLE_NODE:
        case DROP_VIEW_NODE:
        case DROP_TRIGGER_NODE:
            return qualifiedDDLNode((DDLStatementNode)node);
        case DROP_INDEX_NODE:
            return dropIndexNode((DropIndexNode)node);
        case EXPLAIN_STATEMENT_NODE:
            return explainStatementNode((ExplainStatementNode)node);
        case TRANSACTION_CONTROL_NODE:
            return transactionControlNode((TransactionControlNode)node);
        case SET_TRANSACTION_ISOLATION_NODE:
            return setTransactionIsolationNode((SetTransactionIsolationNode)node);
        case SET_TRANSACTION_ACCESS_NODE:
            return setTransactionAccessNode((SetTransactionAccessNode)node);
        case SET_CONFIGURATION_NODE:
            return setConfigurationNode((SetConfigurationNode)node);
        case TABLE_ELEMENT_LIST:
            return tableElementList((TableElementList)node);
        case COLUMN_DEFINITION_NODE:
            return columnDefinitionNode((ColumnDefinitionNode)node);
        case CONSTRAINT_DEFINITION_NODE:
            return constraintDefinitionNode((ConstraintDefinitionNode)node);
        case FK_CONSTRAINT_DEFINITION_NODE:
            return fkConstraintDefinitionNode((FKConstraintDefinitionNode)node);
        case CREATE_INDEX_NODE:
            return createIndexNode((CreateIndexNode)node);
        case INDEX_COLUMN_LIST:
            return indexColumnList((IndexColumnList)node);
        case INDEX_COLUMN:
            return indexColumn((IndexColumn)node);
        case CREATE_ALIAS_NODE:
            return createAliasNode((CreateAliasNode)node);
        case RENAME_NODE:
            return renameNode((RenameNode)node);
        case CURSOR_NODE:
            return cursorNode((CursorNode)node);
        case SELECT_NODE:
            return selectNode((SelectNode)node);
        case INSERT_NODE:
            return insertNode((InsertNode)node);
        case UPDATE_NODE:
            return updateNode((UpdateNode)node);
        case DELETE_NODE:
            return deleteNode((DeleteNode)node);
        case SUBQUERY_NODE:
            return subqueryNode((SubqueryNode)node);
        case RESULT_COLUMN_LIST:
            return resultColumnList((ResultColumnList)node);
        case RESULT_COLUMN:
            return resultColumn((ResultColumn)node);
        case ALL_RESULT_COLUMN:
            return allResultColumn((AllResultColumn)node);
        case FROM_LIST:
            return fromList((FromList)node);
        case JOIN_NODE:
        case HALF_OUTER_JOIN_NODE:
        case FULL_OUTER_JOIN_NODE:
            return joinNode((JoinNode)node);
        case UNION_NODE:
            return unionNode((UnionNode)node);
        case GROUP_BY_LIST:
            return groupByList((GroupByList)node);
        case GROUP_CONCAT_NODE:
            return groupConcat((GroupConcatNode)node);
        case ORDER_BY_LIST:
            return orderByList((OrderByList)node);
        case VALUE_NODE_LIST:
            return valueNodeList((ValueNodeList)node);
        case FROM_BASE_TABLE:
            return fromBaseTable((FromBaseTable)node);
        case FROM_SUBQUERY:
            return fromSubquery((FromSubquery)node);
        case TABLE_NAME:
            return tableName((TableName)node);
        case COLUMN_REFERENCE:
            return columnReference((ColumnReference)node);
        case VIRTUAL_COLUMN_NODE:
            return virtualColumnNode((VirtualColumnNode)node);
        case ROW_RESULT_SET_NODE:
            return rowResultSetNode((RowResultSetNode)node);
        case ROWS_RESULT_SET_NODE:
            return rowsResultSetNode((RowsResultSetNode)node);
        case GROUP_BY_COLUMN:
            return groupByColumn((GroupByColumn)node);
        case ORDER_BY_COLUMN:
            return orderByColumn((OrderByColumn)node);
        case PARTITION_BY_LIST:
            return partitionByList((PartitionByList)node);
        case PARTITION_BY_COLUMN:
            return partitionByColumn((PartitionByColumn)node);
        case WINDOW_DEFINITION_NODE:
            return windowDefinitionNode((WindowDefinitionNode)node);
        case WINDOW_REFERENCE_NODE:
            return windowReferenceNode((WindowReferenceNode)node);
        case AGGREGATE_WINDOW_FUNCTION_NODE:
            return aggregateWindowFunctionNode((AggregateWindowFunctionNode)node);
        case ROW_NUMBER_FUNCTION_NODE:
            return rowNumberFunctionNode((RowNumberFunctionNode)node);
        case AND_NODE:
        case OR_NODE:
            return binaryLogicalOperatorNode((BinaryLogicalOperatorNode)node);
        case BINARY_EQUALS_OPERATOR_NODE:
        case BINARY_NOT_EQUALS_OPERATOR_NODE:
        case BINARY_GREATER_THAN_OPERATOR_NODE:
        case BINARY_GREATER_EQUALS_OPERATOR_NODE:
        case BINARY_LESS_THAN_OPERATOR_NODE:
        case BINARY_LESS_EQUALS_OPERATOR_NODE:
            return binaryComparisonOperatorNode((BinaryComparisonOperatorNode)node);
        case BINARY_PLUS_OPERATOR_NODE:
        case BINARY_TIMES_OPERATOR_NODE:
        case BINARY_DIVIDE_OPERATOR_NODE:
        case BINARY_DIV_OPERATOR_NODE:
        case BINARY_MINUS_OPERATOR_NODE:
        case MOD_OPERATOR_NODE:
            return binaryArithmeticOperatorNode((BinaryArithmeticOperatorNode)node);
        case BINARY_BIT_OPERATOR_NODE:
            return binaryBitOperatorNode((BinaryBitOperatorNode)node);
        case CONCATENATION_OPERATOR_NODE:
            return concatenationOperatorNode((ConcatenationOperatorNode)node);
        case NOT_NODE:
            return notNode((NotNode)node);
        case IS_NULL_NODE:
        case IS_NOT_NULL_NODE:
            return isNullNode((IsNullNode)node);
        case IS_NODE:
            return isNode((IsNode)node);
        case ABSOLUTE_OPERATOR_NODE:
        case SQRT_OPERATOR_NODE:
            return unaryArithmeticOperatorNode((UnaryArithmeticOperatorNode)node);
        case UNARY_PLUS_OPERATOR_NODE:
        case UNARY_MINUS_OPERATOR_NODE:
            return unaryPrefixOperatorNode((UnaryArithmeticOperatorNode)node);
        case UNARY_BITNOT_OPERATOR_NODE:
            return unaryBitOperatorNode((UnaryBitOperatorNode)node);
        case UNARY_DATE_TIMESTAMP_OPERATOR_NODE:
          return unaryDateTimestampOperatorNode((UnaryDateTimestampOperatorNode)node);
        case TIMESTAMP_OPERATOR_NODE:
          return timestampOperatorNode((TimestampOperatorNode)node);
        case EXTRACT_OPERATOR_NODE:
          return extractOperatorNode((ExtractOperatorNode)node);
        case CHAR_LENGTH_OPERATOR_NODE:
          return lengthOperatorNode((LengthOperatorNode)node);
        case OCTET_LENGTH_OPERATOR_NODE:
          return octetLengthOperatorNode((OctetLengthOperatorNode)node);
        case RIGHT_FN_NODE:
        case LEFT_FN_NODE:
            return leftRightFuncOperatorNode((LeftRightFuncOperatorNode)node);
        case SIMPLE_STRING_OPERATOR_NODE:
            return simpleStringOperatorNode((SimpleStringOperatorNode)node);
        case LIKE_OPERATOR_NODE:
            return likeEscapeOperatorNode((LikeEscapeOperatorNode)node);
        case LOCATE_FUNCTION_NODE:
        case SUBSTRING_OPERATOR_NODE:
            return ternaryOperatorNode((TernaryOperatorNode)node);
        case TIMESTAMP_ADD_FN_NODE:
        case TIMESTAMP_DIFF_FN_NODE:
            return timestampFunctionNode((TernaryOperatorNode)node);
        case TRIM_OPERATOR_NODE:
            return trimOperatorNode((TrimOperatorNode)node);
        case IN_LIST_OPERATOR_NODE:
            return inListOperatorNode((InListOperatorNode)node);
        case ROW_CTOR_NODE:
            return rowCtorNode((RowConstructorNode)node);
        case BETWEEN_OPERATOR_NODE:
            return betweenOperatorNode((BetweenOperatorNode)node);
        case CONDITIONAL_NODE:
            return conditionalNode((ConditionalNode)node);
        case SIMPLE_CASE_NODE:
            return simpleCaseNode((SimpleCaseNode)node);
        case COALESCE_FUNCTION_NODE:
            return coalesceFunctionNode((CoalesceFunctionNode)node);
        case AGGREGATE_NODE:
            return aggregateNode((AggregateNode)node);
        case UNTYPED_NULL_CONSTANT_NODE:
        case SQL_BOOLEAN_CONSTANT_NODE:
        case BOOLEAN_CONSTANT_NODE:
        case BIT_CONSTANT_NODE:
        case VARBIT_CONSTANT_NODE:
        case CHAR_CONSTANT_NODE:
        case DECIMAL_CONSTANT_NODE:
        case DOUBLE_CONSTANT_NODE:
        case FLOAT_CONSTANT_NODE:
        case INT_CONSTANT_NODE:
        case LONGINT_CONSTANT_NODE:
        case LONGVARBIT_CONSTANT_NODE:
        case LONGVARCHAR_CONSTANT_NODE:
        case SMALLINT_CONSTANT_NODE:
        case TINYINT_CONSTANT_NODE:
        case USERTYPE_CONSTANT_NODE:
        case VARCHAR_CONSTANT_NODE:
        case BLOB_CONSTANT_NODE:
        case CLOB_CONSTANT_NODE:
        case XML_CONSTANT_NODE:
            return constantNode((ConstantNode)node);
        case PARAMETER_NODE:
            return parameterNode((ParameterNode)node);
        case DEFAULT_NODE:
            return "DEFAULT";
        case USER_NODE:
            return "USER";
        case CURRENT_USER_NODE:
            return "CURRENT_USER";
        case SESSION_USER_NODE:
            return "SESSION_USER";
        case SYSTEM_USER_NODE:
            return "SYSTEM_USER";
        case CURRENT_ISOLATION_NODE:
            return "CURRENT ISOLATION";
        case IDENTITY_VAL_NODE:
            return "IDENTITY_VAL_LOCAL()";
        case CURRENT_SCHEMA_NODE:
            return "CURRENT SCHEMA";
        case CURRENT_ROLE_NODE:
            return "CURRENT_ROLE";
        case CURRENT_DATETIME_OPERATOR_NODE:
            return currentDatetimeOperatorNode((CurrentDatetimeOperatorNode)node);
        case CAST_NODE:
            return castNode((CastNode)node);
        case EXPLICIT_COLLATE_NODE:
            return explicitCollateNode((ExplicitCollateNode)node);
        case NEXT_SEQUENCE_NODE:
            return nextSequenceNode((NextSequenceNode)node);
        case CURRENT_SEQUENCE_NODE:
            return currentSequenceNode((CurrentSequenceNode)node);
        case JAVA_TO_SQL_VALUE_NODE:
            return javaToSQLValueNode((JavaToSQLValueNode)node);
        case SQL_TO_JAVA_VALUE_NODE:
            return sqlToJavaValueNode((SQLToJavaValueNode)node);
        case STATIC_METHOD_CALL_NODE:
            return staticMethodCallNode((StaticMethodCallNode)node);
        case CALL_STATEMENT_NODE:
            return callStatementNode((CallStatementNode)node);
        case INDEX_CONSTRAINT_NODE:
            return indexConstraint((IndexConstraintDefinitionNode)node);
        case DECLARE_STATEMENT_NODE:
            return declareStatementNode((DeclareStatementNode)node);
        case FETCH_STATEMENT_NODE:
            return fetchStatementNode((FetchStatementNode)node);
        case CLOSE_STATEMENT_NODE:
            return closeStatementNode((CloseStatementNode)node);
        case PREPARE_STATEMENT_NODE:
            return prepareStatementNode((PrepareStatementNode)node);
        case EXECUTE_STATEMENT_NODE:
            return executeStatementNode((ExecuteStatementNode)node);
        case DEALLOCATE_STATEMENT_NODE:
            return deallocateStatementNode((DeallocateStatementNode)node);
        case COPY_STATEMENT_NODE:
            return copyStatementNode((CopyStatementNode)node);
        case GENERIC_PROPERTIES:
            return genericProperties((GenericProperties)node);
        default:
            return "**UNKNOWN(" + node.getNodeType() +")**";
        }
    }

    protected String createAnalyzerNode(CreateAnalyzerNode node) throws StandardException {
        StringBuilder builder = new StringBuilder("CREATE ANALYZER ");
        builder.append(node.getObjectName().getTableName()).append(' ');
        TableName extendsName = node.getExtendsName();
        if (extendsName != null) {
            builder.append("EXTENDS ").append(extendsName.getTableName()).append(' ');
        }

        builder.append("WITH (");
        AnalyzerElements elements = node.getElements();
        if (elements.hasProperties()) {
            builder.append(toString(elements.getProperties()));
        } else {
            if (elements.getTokenizer() != null) {
                builder.append("TOKENIZER ")
                        .append(toString(elements.getTokenizer()));
            }
            if (elements.getTokenFilters().size() > 0) {
                if (elements.getTokenizer() != null) { builder.append(", "); }
                builder.append("TOKEN_FILTERS WITH (");
                boolean firstTokenFilter = true;
                for (NamedNodeWithOptionalProperties tokenFilter : elements.getTokenFilters()) {
                    if (!firstTokenFilter) {
                        builder.append(", ");
                    } else {
                        firstTokenFilter=false;
                    }
                    builder.append(toString(tokenFilter));
                }
                builder.append(')');
            }
            if (elements.getCharFilters() != null && elements.getCharFilters().size() > 0) {
                if (elements.getTokenFilters().size() > 0 || elements.getTokenizer() != null) {
                    builder.append(", ");
                }
                builder.append("CHAR_FILTERS WITH (");
                boolean firstCharFilter = true;
                for (NamedNodeWithOptionalProperties charFilter : elements.getCharFilters()) {
                    if (!firstCharFilter) {
                        builder.append(", ");
                    } else {
                        firstCharFilter = false;
                    }
                    builder.append(toString(charFilter));
                }
                builder.append(")");
            }
        }
        builder.append(')');
        return builder.toString();
    }

    protected String namedNodeWithOptionalProperties(NamedNodeWithOptionalProperties node)
            throws StandardException {
        StringBuilder builder = new StringBuilder(node.getName());
        GenericProperties properties = node.getProperties();
        if (properties != null && properties.hasProperties()) {
            builder.append(' ').append("WITH (").append(toString(properties)).append(')');
        }
        return builder.toString();
    }

    protected String indexConstraint(IndexConstraintDefinitionNode node) throws StandardException
    {

        StringBuilder builder = new StringBuilder("INDEX ");
        String indexName = node.getIndexName();
        if (indexName != null) { builder.append(indexName).append(' '); }
        if (node.isIndexOff()) {
            builder.append("OFF");
        } else {
            builder.append("USING ").append(node.getIndexMethod());

            builder.append("(")
                   .append(indexColumnList(node.getIndexColumnList()))
                   .append(")");

            if (node.getIndexProperties() != null) {
                builder.append(" WITH (");
                    builder.append(toString(node.getIndexProperties()));
                builder.append(")");
            }
        }
        return builder.toString();
    }

    protected String genericProperties(GenericProperties properties) throws StandardException
    {
        StringBuilder builder = new StringBuilder();
        boolean firstRun = true;
        for (Map.Entry<String, QueryTreeNode> property : properties.iterator()) {
            if (firstRun) {
                firstRun=false;
            } else {
                builder.append(",");
            }
            builder.append(String.format("\"%s\"", property.getKey()))
                    .append("=");
            if (property.getValue() instanceof ValueNode) {
                builder.append(toString(property.getValue()));
            } else if(property.getValue() instanceof ValueNodeList) {
                builder.append("[");
                boolean innerFirstRun = true;
                for (ValueNode valueNode : (ValueNodeList)property.getValue()) {
                    if (innerFirstRun) {
                        innerFirstRun=false;
                    } else {
                        builder.append(",");
                    }
                    builder.append(toString(valueNode));
                }
                builder.append("]");
            }
        }
        return builder.toString();
    }

    protected String createTableNode(CreateTableNode node) throws StandardException {
        StringBuilder str = new StringBuilder("CREATE TABLE ");
        str.append(toString(node.getObjectName()));
        if (node.getTableElementList() != null) {
            str.append("(");
            str.append(toString(node.getTableElementList()));
            str.append(")");
        }
        if (node.getQueryExpression() != null) {
            str.append(" AS (");
            str.append(toString(node.getQueryExpression()));
            str.append(") WITH ");
            if (!node.isWithData()) str.append("NO ");
            str.append("DATA");
        }
        return str.toString();
    }

    protected String createViewNode(CreateViewNode node) throws StandardException {
        StringBuilder str = new StringBuilder("CREATE VIEW ");
        str.append(toString(node.getObjectName()));
        if (node.getResultColumns() != null) {
            str.append("(");
            str.append(toString(node.getResultColumns()));
            str.append(")");
        }
        str.append(" AS (");
        str.append(toString(node.getParsedQueryExpression()));
        str.append(")");
        return str.toString();
    }

    protected String tableElementList(TableElementList node) throws StandardException {
        return nodeList(node);
    }

    protected String columnDefinitionNode(ColumnDefinitionNode node)
            throws StandardException {
        return node.getColumnName() + " " + node.getType();
    }

    protected String constraintDefinitionNode(ConstraintDefinitionNode node) 
            throws StandardException {
        switch (node.getConstraintType()) {
        case PRIMARY_KEY:
            return "PRIMARY KEY(" + toString(node.getColumnList()) + ")";
        case UNIQUE:
            return "UNIQUE(" + toString(node.getColumnList()) + ")";
        case INDEX:
            return indexConstraint((IndexConstraintDefinitionNode)node);
        default:
            return "**UNKNOWN(" + node.getConstraintType() + ")";
        }
    }

    protected String fkConstraintDefinitionNode(FKConstraintDefinitionNode node)
            throws StandardException {
        StringBuilder str = new StringBuilder();
        if (node.isGrouping())
            str.append("GROUPING ");
        str.append("FOREIGN KEY(");
        str.append(toString(node.getColumnList()));
        str.append(") REFERENCES ");
        str.append(toString(node.getRefTableName()));
        str.append("(");
        str.append(toString(node.getColumnList()));
        str.append(")");
        return str.toString();
    }

    protected String createIndexNode(CreateIndexNode node) throws StandardException {
        StringBuilder str = new StringBuilder("CREATE ");
        str.append("INDEX");
        str.append(" ");
        
        switch (node.getExistenceCheck())
        {
            case IF_EXISTS:
                str.append("IF EXISTS ");
                break;
            case IF_NOT_EXISTS:
                str.append("IF NOT EXISTS ");
                break;  
        }

        str.append(toString(node.getIndexName()));
        str.append(" ON ");
        str.append(node.getIndexTableName());
        str.append(" USING ");
        str.append(node.getIndexMethod());
        str.append("(");
        str.append(toString(node.getColumnList()));
        str.append(")");
        if (node.getIndexProperties() != null) {
            str.append(" WITH (");
            str.append(toString(node.getIndexProperties()));
            str.append(")");
        }
        return str.toString();
    }

    protected String indexColumnList(IndexColumnList node) throws StandardException {

        StringBuilder buffer = new StringBuilder();
        int firstFunctionArg = node.firstFunctionArg();
        int lastFunctionArg = node.lastFunctionArg();
        int arg = 0;
        while (arg < node.size()) {
            if (arg > 0) {
                buffer.append(", ");
            }
            if (arg == firstFunctionArg) {
                buffer.append(node.functionType());
                buffer.append('(');
            }
            buffer.append(toString(node.get(arg)));
            if (arg == lastFunctionArg) {
                buffer.append(')');
            }
            arg++;
        }
        return buffer.toString();
    }

    protected String indexColumn(IndexColumn node) throws StandardException {
        StringBuilder str = new StringBuilder();
        if (node.getTableName() != null) {
            str.append(toString(node.getTableName()));
            str.append(".");
        }
        str.append(node.getColumnName());
        if (!node.isAscending())
            str.append(" DESC");
        return str.toString();
    }

    protected String createAliasNode(CreateAliasNode node) throws StandardException {
        StringBuilder str = new StringBuilder(node.statementToString());
        if (node.isCreateOrReplace())
            str.insert(6, " OR REPLACE");
        str.append(' ');
        str.append(toString(node.getObjectName()));
        switch (node.getAliasType()) {
        case PROCEDURE:
        case FUNCTION:
            str.append(node.getAliasInfo());
            if (node.getDefinition() != null) {
                str.append(" AS '");
                if (node.getDefinition().indexOf('\n') >= 0) {
                    str.append("$$");
                    str.append(node.getDefinition());
                    str.append("$$");
                }
                else {
                    str.append(node.getDefinition().replace("'", "''"));
                }
                str.append('\'');
            }
            else {
                str.append(" EXTERNAL NAME '");
                str.append(node.getJavaClassName());
                if (node.getMethodName() != null) {
                    str.append('.');
                    str.append(node.getMethodName());
                }
                str.append('\'');
            }
            break;
        }
        return str.toString();
    }

    protected String renameNode(RenameNode node) throws StandardException {
        if (node.isAlterTable()) {
            return "ALTER TABLE " + toString(node.getObjectName()) +
                "RENAME COLUMN " + node.getOldObjectName() +
                " TO " + node.getNewObjectName();
        }
        else if (node.getRenameType() == RenameNode.RenameType.INDEX
                    || node.getRenameType() == RenameNode.RenameType.COLUMN) {
            if (node.getObjectName() == null) {
                return node.statementToString() + " " + node.getOldObjectName() +
                    " TO " + node.getNewObjectName();
            }
            else {
                return node.statementToString() + " " + toString(node.getObjectName()) +
                    "." + node.getOldObjectName() +
                    " TO " + node.getNewObjectName();
            }
        }
        else {
            return node.statementToString() + " " + toString(node.getObjectName()) +
                " TO " + toString(node.getNewTableName());
        }
    }

    protected String dropIndexNode(DropIndexNode node) throws StandardException {
        StringBuilder str = new StringBuilder(node.statementToString());
        str.append(" ");
        if (node.getObjectName() != null) {
            str.append(toString(node.getObjectName()));
            str.append(".");
        }
        str.append(node.getIndexName());
        return str.toString();
    }

    protected String cursorNode(CursorNode node) throws StandardException {
        String result = toString(node.getResultSetNode());
        if (node.getOrderByList() != null) {
            result += " " + toString(node.getOrderByList());
        }
        if (node.getFetchFirstClause() != null) {
            result += " LIMIT " + toString(node.getFetchFirstClause());
        }
        if (node.getOffsetClause() != null) {
            result += " OFFSET " + toString(node.getOffsetClause());
        }
        return result;
    }

    protected String selectNode(SelectNode node) throws StandardException {
        StringBuilder str = new StringBuilder("SELECT ");
        if (node.isDistinct())
            str.append("DISTINCT ");
        str.append(toString(node.getResultColumns()));
        if (!node.getFromList().isEmpty()) {
            str.append(" FROM ");
            str.append(toString(node.getFromList()));
        }
        if (node.getWhereClause() != null) {
            str.append(" WHERE ");
            str.append(toString(node.getWhereClause()));
        }
        if (node.getGroupByList() != null) {
            str.append(" ");
            str.append(toString(node.getGroupByList()));
        }
        if (node.getHavingClause() != null) {
            str.append(" HAVING ");
            str.append(toString(node.getHavingClause()));
        }
        if (node.getWindows() != null) {
            str.append(" ");
            str.append(windowList(node.getWindows())); // Does not have NodeType.
        }
        return str.toString();
    }

    protected String insertNode(InsertNode node) throws StandardException {
        StringBuilder str = new StringBuilder("INSERT INTO ");
        str.append(toString(node.getTargetTableName()));
        if (node.getTargetColumnList() != null) {
            str.append("(");
            str.append(toString(node.getTargetColumnList()));
            str.append(")");
        }
        str.append(" ");
        str.append(toString(node.getResultSetNode()));
        if (node.getOrderByList() != null) {
            str.append(" ");
            str.append(toString(node.getOrderByList()));
        }
        if (node.getReturningList() != null) {
            str.append(" RETURNING ");
            str.append(toString(node.getReturningList()));
        }
        return str.toString();
    }

    protected String updateNode(UpdateNode unode) throws StandardException {
        // Cf. Parser's getUpdateNode().
        SelectNode snode = (SelectNode)unode.getResultSetNode();
        StringBuilder str = new StringBuilder("UPDATE ");
        str.append(toString(snode.getFromList().get(0)));
        str.append(" SET ");
        boolean first = true;
        for (ResultColumn col : snode.getResultColumns()) {
            if (first)
                first = false;
            else
                str.append(", ");
            str.append(toString(col.getReference()));
            str.append(" = ");
            str.append(maybeParens(col.getExpression()));
        }
        if (snode.getWhereClause() != null) {
            str.append(" WHERE ");
            str.append(toString(snode.getWhereClause()));
        }
        if (unode.getReturningList() != null) {
            str.append(" RETURNING ");
            str.append(toString(unode.getReturningList()));
        }
        return str.toString();
    }

    protected String deleteNode(DeleteNode dnode) throws StandardException {
        // Cf. Parser's getDeleteNode().
        SelectNode snode = (SelectNode)dnode.getResultSetNode();
        StringBuilder str = new StringBuilder("DELETE FROM ");
        str.append(toString(snode.getFromList().get(0)));
        if (snode.getWhereClause() != null) {
            str.append(" WHERE ");
            str.append(toString(snode.getWhereClause()));
        }
        if (dnode.getReturningList() != null) {
            str.append(" RETURNING ");
            str.append(toString(dnode.getReturningList()));
        }
        return str.toString();
    }

    protected String subqueryNode(SubqueryNode node) throws StandardException {
        String str = toString(node.getResultSet());
        if (node.getOrderByList() != null) {
            str = str + " " + toString(node.getOrderByList());
        }
        str = "(" + str + ")";
        switch (node.getSubqueryType()) {
        case FROM:
        case EXPRESSION:
        default:
            return str;
        case EXISTS:
            return "EXISTS " + str;
        case NOT_EXISTS:
            return "NOT EXISTS " + str;
        case IN:
            return maybeParens(node.getLeftOperand()) + " IN " + str;
        case NOT_IN:
            return maybeParens(node.getLeftOperand()) + " NOT IN " + str;
        case EQ_ANY:
            return maybeParens(node.getLeftOperand()) + " = ANY " + str;
        case EQ_ALL:
            return maybeParens(node.getLeftOperand()) + " = ALL " + str;
        case NE_ANY:
            return maybeParens(node.getLeftOperand()) + " <> ANY " + str;
        case NE_ALL:
            return maybeParens(node.getLeftOperand()) + " <> ALL " + str;
        case GT_ANY:
            return maybeParens(node.getLeftOperand()) + " > ANY " + str;
        case GT_ALL:
            return maybeParens(node.getLeftOperand()) + " > ALL " + str;
        case GE_ANY:
            return maybeParens(node.getLeftOperand()) + " >= ANY " + str;
        case GE_ALL:
            return maybeParens(node.getLeftOperand()) + " > ANY " + str;
        case LT_ANY:
            return maybeParens(node.getLeftOperand()) + " < ANY " + str;
        case LT_ALL:
            return maybeParens(node.getLeftOperand()) + " < ALL " + str;
        case LE_ANY:
            return maybeParens(node.getLeftOperand()) + " <= ANY " + str;
        case LE_ALL:
            return maybeParens(node.getLeftOperand()) + " <= ALL " + str;
        }
    }

    protected String rowResultSetNode(RowResultSetNode node) throws StandardException {
        return "VALUES(" + toString(node.getResultColumns()) + ")";
    }

    protected String rowsResultSetNode(RowsResultSetNode node) throws StandardException {
        StringBuilder str = new StringBuilder("VALUES");
        boolean first = true;
        for (RowResultSetNode row : node.getRows()) {
            if (first)
                first = false;
            else
                str.append(", ");
            str.append("(");
            str.append(toString(row.getResultColumns()));
            str.append(")");
        }
        return str.toString();
    }

    protected String resultColumnList(ResultColumnList node) throws StandardException {
        return nodeList(node);
    }
    
    protected String resultColumn(ResultColumn node) throws StandardException {
        if (node.getReference() != null)
            return toString(node.getReference());

        String n = node.getName();
        if (node.getExpression() == null)
            return n;

        String x = maybeParens(node.getExpression());
        if ((n == null) || n.equals(x))
            return x;
        else
            return x + " AS " + n;
    }

    protected String allResultColumn(AllResultColumn node) throws StandardException {
        return "*";
    }

    protected String fromList(FromList node) throws StandardException {
        return nodeList(node);
    }

    protected String fromBaseTable(FromBaseTable node) throws StandardException {
        String tn = toString(node.getOrigTableName());
        String n = node.getCorrelationName();
        if (n == null)
            return tn;
        else
            return tn + " AS " + n;
    }

    protected String fromSubquery(FromSubquery node) throws StandardException {
        StringBuilder str = new StringBuilder(toString(node.getSubquery()));
        if (node.getOrderByList() != null) {
            str.append(' ');
            str.append(toString(node.getOrderByList()));
        }
        str.insert(0, '(');
        str.append(')');
        str.append(" AS ");
        str.append(node.getCorrelationName());
        if (node.getResultColumns() != null) {
            str.append('(');
            str.append(toString(node.getResultColumns()));
            str.append(')');
        }
        return str.toString();
    }

    protected String joinNode(JoinNode node) throws StandardException {
        StringBuilder str = new StringBuilder(toString(node.getLeftResultSet()));
        JoinNode.JoinType joinType = JoinNode.JoinType.INNER;
        if (node instanceof HalfOuterJoinNode)
            joinType = ((HalfOuterJoinNode)node).isRightOuterJoin() ? 
                JoinNode.JoinType.RIGHT_OUTER : JoinNode.JoinType.LEFT_OUTER;
        else if (node instanceof FullOuterJoinNode)
            joinType = JoinNode.JoinType.FULL_OUTER;
        str.append(' ');
        if (node.isNaturalJoin())
            str.append("NATURAL ");
        str.append(JoinNode.joinTypeToString(joinType));
        str.append(' ');
        str.append(toString(node.getRightResultSet()));
        if (node.getJoinClause() != null) {
            str.append(" ON ");
            str.append(maybeParens(node.getJoinClause()));
        }
        if (node.getUsingClause() != null) {
            str.append(" USING (");
            str.append(toString(node.getUsingClause()));
            str.append(')');
        }
        return str.toString();
    }

    protected String unionNode(UnionNode node) throws StandardException {
        return toString(node.getLeftResultSet()) + " UNION " + 
               toString(node.getRightResultSet());
    }

    protected String tableName(TableName node) throws StandardException {
        return node.getFullTableName();
    }

    protected String columnReference(ColumnReference node) throws StandardException {
        return node.getSQLColumnName();
    }

    protected String virtualColumnNode(VirtualColumnNode node) throws StandardException {
        return node.getSourceColumn().getName();
    }

    protected String groupByList(GroupByList node) throws StandardException {
        return "GROUP BY " + nodeList(node);
    }

    protected String groupByColumn(GroupByColumn node) throws StandardException {
        return maybeParens(node.getColumnExpression());
    }

    protected String orderByList(OrderByList node) throws StandardException {
        return "ORDER BY " + nodeList(node);
    }

    protected String orderByColumn(OrderByColumn node) throws StandardException {
        String result = maybeParens(node.getExpression());
        if (!node.isAscending()) {
            result += " DESC";
        }
        if (node.isNullsOrderedLow()) {
            result += " NULLS FIRST";
        }
        return result;
    }

    protected String partitionByList(PartitionByList node) throws StandardException {
        return "PARTITION BY " + nodeList(node);
    }

    protected String partitionByColumn(PartitionByColumn node) throws StandardException {
        return toString(node.getColumnExpression());
    }

    protected String windowList(WindowList node) throws StandardException {
        return "WINDOW " + nodeList(node);
    }

    protected String windowDefinitionNode(WindowDefinitionNode node)
            throws StandardException {
        StringBuffer str = new StringBuffer("");
        if (!node.isInline()) {
            str.append(node.getName());
            str.append(" AS ");
        }
        str.append("(");
        if (node.getPartitionByList() != null)
            str.append(toString(node.getPartitionByList()));
        if (node.getOrderByList() != null) {
            if (node.getPartitionByList() != null)
                str.append(" ");
            str.append(toString(node.getOrderByList()));
        }
        str.append(")");
        return str.toString();
    }

    protected String windowReferenceNode(WindowReferenceNode node)
            throws StandardException {
        return node.getName();
    }
    
    protected String aggregateWindowFunctionNode(AggregateWindowFunctionNode node)
            throws StandardException {
        return toString(node.getAggregateFunction()) + 
            " OVER " + toString(node.getWindow());
    }

    protected String rowNumberFunctionNode(RowNumberFunctionNode node)
            throws StandardException {
        return node.getOperator().toUpperCase() + "()" +
            " OVER " + toString(node.getWindow());
    }

    protected String binaryLogicalOperatorNode(BinaryLogicalOperatorNode node) 
            throws StandardException {
        return infixBinary(node);
    }

    protected String binaryComparisonOperatorNode(BinaryComparisonOperatorNode node)
        throws StandardException {
        return infixBinary(node);
    }

    protected String binaryArithmeticOperatorNode(BinaryArithmeticOperatorNode node) 
            throws StandardException {
        return infixBinary(node);
    }

    protected String binaryBitOperatorNode(BinaryBitOperatorNode node) 
            throws StandardException {
        return infixBinary(node);
    }

    protected String concatenationOperatorNode(ConcatenationOperatorNode node)
            throws StandardException {
        return infixBinary(node);
    }

    protected String leftRightFuncOperatorNode(LeftRightFuncOperatorNode node)
            throws StandardException {
        return functionBinary(node);
    }

    protected String simpleStringOperatorNode(SimpleStringOperatorNode node)
            throws StandardException {
        return functionUnary(node);
    }

    protected String notNode(NotNode node) throws StandardException {
        return prefixUnary(node);
    }

    protected String isNullNode(IsNullNode node) throws StandardException {
        return suffixUnary(node);
    }

    protected String unaryArithmeticOperatorNode(UnaryArithmeticOperatorNode node) 
            throws StandardException {
        return functionUnary(node);
    }

    protected String unaryPrefixOperatorNode(UnaryArithmeticOperatorNode node) 
            throws StandardException {
        return prefixUnary(node);
    }

    protected String unaryBitOperatorNode(UnaryBitOperatorNode node) 
            throws StandardException {
        return prefixUnary(node);
    }

    protected String extractOperatorNode(ExtractOperatorNode node) 
            throws StandardException {
        return node.getOperator().substring("EXTRACT ".length()).toUpperCase() + "(" +
            toString(node.getOperand()) + ")";
    }

    protected String unaryDateTimestampOperatorNode(UnaryDateTimestampOperatorNode node) 
            throws StandardException {
        return functionUnary(node);
    }

    protected String timestampOperatorNode(TimestampOperatorNode node) 
            throws StandardException {
        return functionBinary(node);
    }

    protected String lengthOperatorNode(LengthOperatorNode node) 
            throws StandardException {
        return functionUnary(node);
    }

    protected String octetLengthOperatorNode(OctetLengthOperatorNode node) 
            throws StandardException {
        return functionUnary(node);
    }

    protected String isNode(IsNode node) throws StandardException {
        StringBuilder str = new StringBuilder(maybeParens(node.getLeftOperand()));
        str.append(" IS ");
        if (node.isNegated())
            str.append("NOT ");
        ValueNode rightOperand = node.getRightOperand();
        if (rightOperand instanceof BooleanConstantNode) {
            Boolean value = (Boolean)((BooleanConstantNode)rightOperand).getValue();
            if (value == null)
                str.append("UNKNOWN");
            else
                str.append(value.toString().toUpperCase());
        }
        else
            str.append(maybeParens(rightOperand));
        return str.toString();
    }

    protected String aggregateNode(AggregateNode node) throws StandardException {
        if (node.getOperand() == null)
            return node.getAggregateName();
        else
            return node.getAggregateName() + "(" + toString(node.getOperand()) + ")";
    }

    protected String likeEscapeOperatorNode(LikeEscapeOperatorNode node) 
            throws StandardException {
        String like = maybeParens(node.getReceiver()) +
            " " + node.getOperator().toUpperCase() + " " +
            maybeParens(node.getLeftOperand());
        if (node.getRightOperand() != null)
            like += " ESCAPE " + maybeParens(node.getRightOperand());
        return like;
    }

    protected String ternaryOperatorNode(TernaryOperatorNode node)
            throws StandardException {
        StringBuilder str = new StringBuilder(node.getOperator().toUpperCase());
        str.append("(");
        str.append(toString(node.getReceiver()));
        str.append(", ");
        str.append(toString(node.getLeftOperand()));
        if (node.getRightOperand() != null) {
            str.append(", ");
            str.append(toString(node.getRightOperand()));
        }
        return str.toString();
    }

    protected String timestampFunctionNode(TernaryOperatorNode node)
            throws StandardException {
        String interval = toString(node.getReceiver());
        switch ((Integer)((ConstantNode)node.getReceiver()).getValue()) {
        case TernaryOperatorNode.YEAR_INTERVAL:
            interval = "YEAR";
            break;
        case TernaryOperatorNode.QUARTER_INTERVAL:
            interval = "QUARTER";
            break;
        case TernaryOperatorNode.MONTH_INTERVAL:
            interval = "MONTH";
            break;
        case TernaryOperatorNode.WEEK_INTERVAL:
            interval = "WEEK";
            break;
        case TernaryOperatorNode.DAY_INTERVAL:
            interval = "DAY";
            break;
        case TernaryOperatorNode.HOUR_INTERVAL:
            interval = "HOUR";
            break;
        case TernaryOperatorNode.MINUTE_INTERVAL:
            interval = "MINUTE";
            break;
        case TernaryOperatorNode.SECOND_INTERVAL:
            interval = "SECOND";
            break;
        case TernaryOperatorNode.FRAC_SECOND_INTERVAL:
            interval = "MICROSECOND>";
            break;
        }
        return node.getOperator().toUpperCase() + "(" +
            interval + ", " +
            toString(node.getLeftOperand()) + ", " +
            toString(node.getRightOperand()) + ")";
    }

    protected String trimOperatorNode(TrimOperatorNode node)
            throws StandardException {
        if ((node.getRightOperand() instanceof ConstantNode) &&
            " ".equals(((ConstantNode)node.getRightOperand()).getValue())) {
            return node.getOperator().toUpperCase() + "(" +
                toString(node.getLeftOperand()) + ")";
        }
        else {
            StringBuilder str = new StringBuilder("TRIM(");
            if ("LTRIM".equals(node.getOperator()))
                str.append("LEADING");
            else if ("RTRIM".equals(node.getOperator()))
                str.append("TRAILING");
            else
                str.append("BOTH");
            str.append(" ");
            str.append(toString(node.getRightOperand()));
            str.append(" FROM ");
            str.append(toString(node.getLeftOperand()));
            return str.toString();
        }
    }

    protected String inListOperatorNode(InListOperatorNode node) throws StandardException {
        return maybeParens(node.getLeftOperand()) +
            " " + (node.isNegated() ? "NOT IN" : "IN") + 
            " (" + toString(node.getRightOperandList()) + ")";
    }

    protected String valueNodeList(ValueNodeList node) throws StandardException {
        return nodeList(node, true);
    }

    protected String betweenOperatorNode(BetweenOperatorNode node)
            throws StandardException {
        return maybeParens(node.getLeftOperand()) +
            " BETWEEN " + maybeParens(node.getRightOperandList().get(0)) +
            " AND " + maybeParens(node.getRightOperandList().get(1));
    }

    protected String conditionalNode(ConditionalNode node) throws StandardException {
        StringBuilder str = new StringBuilder("CASE");
        while (true) {
            str.append(" WHEN ");
            str.append(maybeParens(node.getTestCondition()));
            str.append(" THEN ");
            str.append(maybeParens(node.getThenNode()));
            ValueNode elseNode = node.getElseNode();
            if (elseNode instanceof ConditionalNode)
                node = (ConditionalNode)elseNode;
            else {
                str.append(" ELSE ");
                str.append(maybeParens(elseNode));
                break;
            }
        }
        str.append(" END");
        return str.toString();
    }

    protected String simpleCaseNode(SimpleCaseNode node) throws StandardException {
        StringBuilder str = new StringBuilder("CASE ");
        str.append(maybeParens(node.getOperand()));
        for (int i = 0; i < node.getNumberOfCases(); i++) {
            str.append(" WHEN ");
            str.append(maybeParens(node.getCaseOperand(i)));
            str.append(" THEN ");
            str.append(maybeParens(node.getResultValue(i)));
        }
        if (node.getElseValue() != null) {
            str.append(" ELSE ");
            str.append(maybeParens(node.getElseValue()));
        }
        str.append(" END");
        return str.toString();
    }

    protected String coalesceFunctionNode(CoalesceFunctionNode node) 
            throws StandardException {
        return functionCall(node.getFunctionName(),
                                                node.getArgumentsList());
    }
    
    protected String constantNode(ConstantNode node) throws StandardException {
        Object value = node.getValue();
        if (value == null)
            return "NULL";
        else if (value instanceof String)
            return "'" + ((String)value).replace("'", "''") + "'";
        else if (value instanceof byte[])
            return hexConstant((byte[])value);
        else if (value instanceof Double)
            return String.format("%e", value);
        else if (value instanceof Boolean)
            return value.toString().toUpperCase();
        else
            return value.toString();
    }

    protected String prefixUnary(UnaryOperatorNode node) throws StandardException {
        return node.getOperator().toUpperCase() + " " +
            maybeParens(node.getOperand());
    }

    protected String suffixUnary(UnaryOperatorNode node) throws StandardException {
        return maybeParens(node.getOperand()) + " " +
            node.getOperator().toUpperCase();
    }

    protected String functionUnary(UnaryOperatorNode node) throws StandardException {
        return node.getOperator().toUpperCase() + "(" +
            toString(node.getOperand()) + ")";
    }

    protected String infixBinary(BinaryOperatorNode node) throws StandardException {
        return maybeParens(node.getLeftOperand()) +
            " " + node.getOperator().toUpperCase() + " " +
            maybeParens(node.getRightOperand());
    }
    
    protected String functionBinary(BinaryOperatorNode node) throws StandardException {
        return node.getOperator().toUpperCase() + "(" +
            toString(node.getLeftOperand()) + ", " +
            toString(node.getRightOperand()) + ")";
    }
    
    protected String functionCall(String functionName, ValueNodeList args)
            throws StandardException {
        return functionName + "(" + nodeList(args, true) + ")";
    }

    protected String nodeList(QueryTreeNodeList<? extends QueryTreeNode> nl)
            throws StandardException {
        return nodeList(nl, false);
    }

    protected String nodeList(QueryTreeNodeList<? extends QueryTreeNode> nl, boolean expr)
            throws StandardException {
        StringBuilder str = new StringBuilder();
        boolean first = true;
        for (QueryTreeNode node : nl) {
            if (first)
                first = false;
            else
                str.append(", ");
            str.append(expr ? maybeParens(node) : toString(node));
        }
        return str.toString();
    }

    protected String maybeParens(QueryTreeNode node) throws StandardException {
        String str = toString(node);
        if (node instanceof ConstantNode)
            return str;
        else if (str.indexOf(' ') < 0)
            return str;
        else
            return "(" + str + ")";
    }

    protected String hexConstant(byte[] value) {
        StringBuilder str = new StringBuilder("X'");
        for (byte b : value) {
            str.append(Integer.toString((int)b & 0xFF, 16).toUpperCase());
        }
        str.append("'");
        return str.toString();
    }

    protected String parameterNode(ParameterNode node) throws StandardException {
        return "$" + (node.getParameterNumber() + 1);
    }

    protected String currentDatetimeOperatorNode(CurrentDatetimeOperatorNode node) 
            throws StandardException {
        switch (node.getField()) {
        case DATE:
            return "CURRENT_DATE";
        case TIME:
            return "CURRENT_TIME";
        case TIMESTAMP:
            return "CURRENT_TIMESTAMP";
        default:
            return "**UNKNOWN(" + node.getField() +")**";
        }
    }

    protected String castNode(CastNode node) throws StandardException {
        return "CAST(" + toString(node.getCastOperand()) + 
            " AS " + node.getType().toString() + ")";
    }

    protected String explicitCollateNode(ExplicitCollateNode node) 
            throws StandardException {
        return maybeParens(node.getOperand()) + 
            " COLLATE " + node.getCollation();
    }

    protected String nextSequenceNode(NextSequenceNode node)
            throws StandardException {
        return "NEXT VALUE FOR " + toString(node.getSequenceName ());
    }

    protected String currentSequenceNode(CurrentSequenceNode node)
            throws StandardException {
        return "CURRENT VALUE FOR " + toString(node.getSequenceName ());
    }

    protected String javaToSQLValueNode(JavaToSQLValueNode node) 
            throws StandardException {
        return toString(node.getJavaValueNode());
    }

    protected String sqlToJavaValueNode(SQLToJavaValueNode node)
            throws StandardException {
        return toString(node.getSQLValueNode());
    }

    protected String staticMethodCallNode(StaticMethodCallNode node)
            throws StandardException {
        StringBuilder str = new StringBuilder();
        if (node.getProcedureName() != null)
            str.append(toString(node.getProcedureName()));
        else
            str.append(node.getMethodName());
        str.append("(");
        JavaValueNode[] params = node.getMethodParameters();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) str.append(", ");
            str.append(maybeParens(params[i]));
        }
        str.append(")");
        return str.toString();
    }

    protected String callStatementNode(CallStatementNode node) throws StandardException {
        return "CALL " + javaToSQLValueNode(node.methodCall());
    }

    protected String qualifiedDDLNode(DDLStatementNode node) throws StandardException {
        return node.statementToString() + " " + node.getObjectName();
    }

    protected String explainStatementNode(ExplainStatementNode node) 
            throws StandardException {
        String detail;
        switch (node.getDetail()) {
        case BRIEF:
            detail = "BRIEF ";
            break;
        case VERBOSE:
            detail = "VERBOSE ";
            break;
        case NORMAL:
        default:
            detail = "";
            break;
        }
        return "EXPLAIN " + detail + toString(node.getStatement());
    }

    protected String transactionControlNode(TransactionControlNode node)
            throws StandardException {
        return node.statementToString();
    }
    
    protected String setTransactionIsolationNode(SetTransactionIsolationNode node)
            throws StandardException {
        return node.statementToString() + " " + node.getIsolationLevel().getSyntax();
    }
    
    protected String setTransactionAccessNode(SetTransactionAccessNode node)
            throws StandardException {
        return node.statementToString() + " " + node.getAccessMode().getSyntax();
    }

    protected String setConfigurationNode(SetConfigurationNode node)
            throws StandardException {
        return node.statementToString() + " = '" + node.getValue() + "'";
    }

    protected String rowCtorNode(RowConstructorNode row) throws StandardException
    {
        ValueNodeList list = row.getNodeList();
        
        switch(list.size())
        {
            case 0:
                return "EMPTY";
            case 1:
                QueryTreeNode node = list.get(0);
                if (!(node instanceof RowConstructorNode))
                    return toString(node);
        }
        
        StringBuilder bd = new StringBuilder();
        for (QueryTreeNode node : list )
        {
            doPrint(node, bd);
            bd.append(", ");
        }

        return bd.substring(0, bd.length() -2); // delete the last (<COMMA> <SPACE>)
    }
    
    protected String declareStatementNode(DeclareStatementNode node) 
            throws StandardException {
        return "DECLARE " + node.getName() + " CURSOR FOR " +
            toString(node.getStatement());
    }

    protected String fetchStatementNode(FetchStatementNode node)
            throws StandardException {
        return "FETCH " + 
            ((node.getCount() < 0) ? "ALL" : Integer.toString(node.getCount())) +
            " FROM " + node.getName();
    }

    protected String closeStatementNode(CloseStatementNode node)
            throws StandardException {
        return "CLOSE " + node.getName();
    }

    protected String prepareStatementNode(PrepareStatementNode node)
            throws StandardException {
        return "PREPARE " + node.getName() + " AS " +
            toString(node.getStatement());
    }

    protected String executeStatementNode(ExecuteStatementNode node)
            throws StandardException {
        return "EXECUTE " + node.getName() + 
            "(" + nodeList(node.getParameterList(), true) + ")";
    }

    protected String deallocateStatementNode(DeallocateStatementNode node)
            throws StandardException {
        return "DEALLOCATE " + node.getName();
    }
     
    protected String copyStatementNode(CopyStatementNode node) 
            throws StandardException {
        StringBuilder str = new StringBuilder("COPY ");
        if (node.getSubquery() != null) {
            str.append("(");
            str.append(toString(node.getSubquery()));
            str.append(")");
        }
        else {
            str.append(node.getTableName());
            if (node.getColumnList() != null) {
                str.append("(");
                str.append(toString(node.getColumnList()));
                str.append(")");
            }
        }
        switch (node.getMode()) {
        case FROM_TABLE:
        case FROM_SUBQUERY:
            str.append(" TO ");
            break;
        case TO_TABLE:
            str.append(" FROM ");
            break;
        }
        if (node.getFilename() != null) {
            str.append(toString(node.getFilename()));
        }
        else if (node.getMode() == CopyStatementNode.Mode.TO_TABLE) {
            str.append("STDIN");
        }
        else {
            str.append("STDOUT");
        }
        boolean options = false;
        if (node.getFormat() != null) {
            options = copyOption(str, "FORMAT", node.getFormat().name(), options);
        }
        if (node.getDelimiter() != null) {
            options = copyOptionString(str, "DELIMITER", node.getDelimiter(), options);
        }
        if (node.getNullString() != null) {
            options = copyOptionString(str, "NULL", node.getNullString(), options);
        }
        if (node.isHeader()) {
            options = copyOption(str, "HEADER", "TRUE", options);
        }
        if (node.getQuote() != null) {
            options = copyOptionString(str, "QUOTE", node.getQuote(), options);
        }
        if (node.getEscape() != null) {
            options = copyOptionString(str, "ESCAPE", node.getEscape(), options);
        }
        if (node.getEncoding() != null) {
            options = copyOptionString(str, "ENCODING", node.getEncoding(), options);
        }
        if (node.getCommitFrequency() != 0) {
            options = copyOption(str, "COMMIT", node.getCommitFrequency() + " ROWS", options);
        }
        if (options) {
            str.append(")");
        }
        return str.toString();
    }

    protected boolean copyOptionString(StringBuilder str, String keyword, String value, boolean options) {
        return copyOption(str, keyword, "'" + value + "'", options);
    }

    protected boolean copyOption(StringBuilder str, String keyword, String value, boolean options) {
        if (!options) {
            str.append(" WITH (");
        }
        else {
            str.append(", ");
        }
        str.append(keyword);
        str.append(" " );
        str.append(value);
        return true;
    }

    protected void doPrint(QueryTreeNode node, StringBuilder bd) throws StandardException
    {
        if (node instanceof RowConstructorNode)
            bd.append(rowCtorNode((RowConstructorNode)node));
        else
            bd.append(toString(node));
    }
    
    protected String groupConcat(GroupConcatNode node) throws StandardException
    {
        StringBuilder ret = new StringBuilder("GROUP_CONCAT(");
        
        ret.append(node.getOperand());
        
        OrderByList orderBy = node.getOrderBy();
        if (orderBy != null)
            ret.append(this.toString(orderBy));
        
        // i
        ret.append("SEPARATOR \'").append(node.getSeparator()).append("\')");
        return ret.toString();
    }
}

package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class QueryPlanner {

    public static final String PRIMARY_KEY_VALUE = "primaryKeyValue";
    public static final String ROUTING_VALUE = "routingValue";

    public static final String SETTINGS_OPTIMIZE_PK_QUERIES = "crate.planner.optimize_pk_queries";

    private Settings settings;

    @Inject
    public QueryPlanner(Settings settings) {
        this.settings = settings;
    }

    /**
     * Check if we can optimize queries based on the SQL WHERE clause.
     * Returns true if Visitor/Generator should stop operating, otherwise false.
     * Besides of the information to stop operating/generating, the planner write optional
     * values to the {@link ParsedStatement}.
     * The {@link org.cratedb.action.sql.TransportSQLAction} can then make decisions using
     * this values if e.g. a {@link org.elasticsearch.action.get.GetRequest} should be used
     * instead of a {@link org.elasticsearch.action.search.SearchRequest}.
     *
     * @param stmt
     * @param node
     * @return
     * @throws StandardException
     */
    public Boolean optimizedWhereClause(ParsedStatement stmt, ValueNode node) throws
            StandardException {

        if (! settings.getAsBoolean(SETTINGS_OPTIMIZE_PK_QUERIES, true)) {
            return false;
        }

        assert stmt.tableContext() != null;

        // First check if we found only one operational node with a primary key.
        // If so we can set the primary key value and return true, so any generator can stop.
        Object primaryKeyValue = extractPrimaryKeyValue(stmt, node);
        if (primaryKeyValue != null) {
            stmt.setPlannerResult(PRIMARY_KEY_VALUE, primaryKeyValue.toString());
            return true;
        }

        // Second check for a primary key in multi-operational nodes
        // If so we can set the routing value but won't return true, generators should finish.
        Object routingValue = extractRoutingValue(stmt, node);
        if (routingValue != null) {
            stmt.setPlannerResult(ROUTING_VALUE, routingValue.toString());
        }

        return false;
    }

    /**
     * If a primary key equals a constant value in the given node, and also is defined as the
     * routing key, return this value.
     *
     * @param stmt
     * @param node
     * @return
     * @throws StandardException
     */
    private Object extractPrimaryKeyValue(ParsedStatement stmt,
                                          ValueNode node) throws StandardException {
        Object value = null;
        if (node.getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
            List<String> primaryKeys = stmt.tableContext().primaryKeysIncludingDefault();
            ValueNode leftOperand = ((BinaryRelationalOperatorNode)node).getLeftOperand();
            ValueNode rightOperand = ((BinaryRelationalOperatorNode)node).getRightOperand();
            if (leftOperand instanceof ColumnReference) {
                if (stmt.tableContext().isRouting(leftOperand.getColumnName()) &&
                        primaryKeys.contains(leftOperand.getColumnName())) {
                    value = stmt.visitor().evaluateValueNode(
                            leftOperand.getColumnName(), rightOperand);
                }
            }
            if (rightOperand instanceof ColumnReference) {
                if (stmt.tableContext().isRouting(rightOperand.getColumnName()) &&
                        primaryKeys.contains(rightOperand.getColumnName())) {
                    value = stmt.visitor().evaluateValueNode(
                            rightOperand.getColumnName(), leftOperand);
                }
            }
        }

        return value;
    }

    /**
     * If a primary key expression is found inside one or more {@link AndNode},
     * return the constant value so it can be used e.g. for routing.
     *
     * @param stmt
     * @param node
     * @return
     * @throws StandardException
     */
    private Object extractRoutingValue(ParsedStatement stmt,
                                       ValueNode node) throws StandardException {
        Object value = null;
        if (node.getNodeType() == NodeTypes.AND_NODE) {
            AndNode andNode = (AndNode)node;
            if (andNode.getLeftOperand().getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
                value = extractPrimaryKeyValue(stmt, andNode.getLeftOperand());
            } else if (andNode.getLeftOperand().getNodeType() == NodeTypes.AND_NODE) {
                value = extractRoutingValue(stmt, andNode.getLeftOperand());
            }
            if (value == null) {
                if (andNode.getRightOperand().getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
                    value = extractPrimaryKeyValue(stmt, andNode.getRightOperand());
                } else if (andNode.getRightOperand().getNodeType() == NodeTypes.AND_NODE) {
                    value = extractRoutingValue(stmt, andNode.getRightOperand());
                }
            }
        }

        return value;
    }

}

package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueryPlanner {

    public static final String PRIMARY_KEY_VALUE = "primaryKeyValue";
    public static final String ROUTING_VALUES = "routingValues";

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

        Set<String> routingValues = new HashSet<>();

        // Second check for a primary key in multi-operational nodes
        // If so we can set the routing value but won't return true, generators should finish.
        Object routingValue = extractRoutingValue(stmt, node);
        if (routingValue != null) {
            routingValues.add(routingValue.toString());
        }

        // Third check for multiple operational nodes with the same primary key.
        // if so we can set the "multiple primary key values"
        // we do not return here, as the generators should finish
        Set<String> orRoutingValues = extractRoutingValuesFromOrClauses(stmt, node);
        if (orRoutingValues != null) {
            routingValues.addAll(orRoutingValues);
        }
        if (!routingValues.isEmpty()) {
            stmt.setPlannerResult(ROUTING_VALUES, routingValues);
        }
        return false;
    }

    /**
     * if we got any number of nested or flat nodes, that contains a primary_key equals a constant value
     * which is also defined as the routing key, combined by OR, we collect all the constant values in a set
     * and return them.
     *
     * This Optimization only returns the values if all nodes are valid, that is, are of the form:
     *
     *   pk=1 OR pk=2 OR pk=3 ...
     *
     * Else we return an empty set
     *
     * @param stmt the ParsedStatement that is checked for Optimization
     * @param node the Parsetree-Node corresponding to the Statements WhereClause
     * @return the set of primary key Values as Strings, if empty, we cannot optimize
     * @throws StandardException
     */
    private Set<String> extractRoutingValuesFromOrClauses(ParsedStatement stmt, ValueNode node) throws StandardException {
        // Hide recursion details
        Set<String> results = new HashSet<>();
        extractRoutingValuesFromOrClauses(stmt, node, results);

        return results;
    }

    private void extractRoutingValuesFromOrClauses(ParsedStatement stmt, ValueNode node, Set<String> results) throws StandardException {

        if (node instanceof OrNode) {
            ValueNode leftOperand = ((OrNode) node).getLeftOperand();
            ValueNode rightOperand = ((OrNode) node).getRightOperand();

            if (leftOperand instanceof OrNode || leftOperand instanceof InListOperatorNode) {
                extractRoutingValuesFromOrClauses(stmt, leftOperand, results);
            } else {
                Object leftValue = extractPrimaryKeyValue(stmt, leftOperand);
                if (leftValue != null) {
                    results.add(leftValue.toString());
                } else {
                    // some invalid node, clear results and quit
                    results.clear();
                    return;
                }
            }

            // shortcut exit if left operand failed
            if (results.isEmpty()) {
                return;
            }

            if (rightOperand instanceof OrNode || rightOperand instanceof InListOperatorNode) {
                extractRoutingValuesFromOrClauses(stmt, rightOperand, results);
            } else {
                Object rightValue = extractPrimaryKeyValue(stmt, rightOperand);
                if (rightValue != null) {
                    results.add(rightValue.toString());
                } else {
                    // some invalid node, clear results and quit
                    results.clear();
                    return;
                }
            }
        } else if (node instanceof InListOperatorNode) {
            // e.g. WHERE pk_col IN (1,2,3,...)
            Object value;
            List<String> primaryKeys = stmt.tableContext().primaryKeysIncludingDefault();
            RowConstructorNode leftOperand = ((InListOperatorNode) node).getLeftOperand();
            RowConstructorNode rightOperandList = ((InListOperatorNode) node).getRightOperandList();
            if (leftOperand.getNodeList().size() == 1 && leftOperand.getNodeList().get(0) instanceof ColumnReference) {
                String columnName = leftOperand.getNodeList().get(0).getColumnName();
                if (stmt.tableContext().isRouting(columnName) && primaryKeys.contains(columnName)) {
                    for (ValueNode listValue : rightOperandList.getNodeList()) {
                        value = stmt.visitor().evaluateValueNode(columnName, listValue);
                        results.add(value.toString());
                    }
                } else {
                    results.clear();
                    return;
                }
            }
        }
    }


    /**
     * If a primary_key equals a constant value in the given node, and also is defined as the
     * routing key we return the constant value this statement asks for.
     *
     * The {@link org.cratedb.action.sql.TransportSQLAction} can then make decisions using
     * this values if e.g. a {@link org.elasticsearch.action.get.GetRequest} should be used
     * instead of a {@link org.elasticsearch.action.search.SearchRequest}.
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
    private Object extractRoutingValue(ParsedStatement stmt, ValueNode node) throws StandardException {
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

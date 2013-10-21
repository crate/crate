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

    // used to mark an exit condition for recursing through or-nodes
    public static class NonOptimizableOrClauseException extends Exception {}

    public static final String PRIMARY_KEY_VALUE = "primaryKeyValue";
    public static final String ROUTING_VALUES = "routingValues";
    public static final String MULTIGET_PRIMARY_KEY_VALUES = "multiGetPrimaryKeyValues";

    public static final String SETTINGS_OPTIMIZE_PK_QUERIES = "crate.planner.optimize_pk_queries";

    private Settings settings;

    @Inject
    public QueryPlanner(Settings settings) {
        this.settings = settings;
    }

    /**
     * take final actions on statement
     */
    public void finalizeWhereClause(ParsedStatement stmt) {
        if ( settings.getAsBoolean(SETTINGS_OPTIMIZE_PK_QUERIES, true)) {
            finalizeMultiGet(stmt);

        }
    }

    /**
     * move MULTIGET_PRIMRY_KEY_VALUES to ROUTING_VALUES if MultiGetRequest is not possible
     * @param stmt the ParsedStatement to operate on
     */
    public void finalizeMultiGet(ParsedStatement stmt) {
        // only leave MULTIGET_PRIMARY_KEY_VALUES as is when we can make a MultiGetRequest
        if ((stmt.getPlannerResult(MULTIGET_PRIMARY_KEY_VALUES) != null && (stmt.nodeType() != NodeTypes.CURSOR_NODE || stmt.hasOrderBy() || stmt.hasGroupBy()))) {
            stmt.setPlannerResult(ROUTING_VALUES, stmt.removePlannerResult(MULTIGET_PRIMARY_KEY_VALUES));
        }
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
        // We put these values in a special plannerResult-slot as we can only decide later what to do with it
        Set<String> orPrimaryKeyValues = extractFromOrClauses(stmt, node);
        if (orPrimaryKeyValues != null && !orPrimaryKeyValues.isEmpty()) {
            stmt.setPlannerResult(MULTIGET_PRIMARY_KEY_VALUES, orPrimaryKeyValues);
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
    private Set<String> extractFromOrClauses(ParsedStatement stmt, ValueNode node) throws StandardException {
        // Hide recursion details
        Set<String> results = new HashSet<>();
        extractFromOrClauses(stmt, node, results);

        return results;
    }

    private void extractFromOrClauses(ParsedStatement stmt, ValueNode node, Set<String> results) throws StandardException {

        if (node.getNodeType() == NodeTypes.OR_NODE) {
            ValueNode leftOperand = ((OrNode) node).getLeftOperand();
            ValueNode rightOperand = ((OrNode) node).getRightOperand();
            try {
                extractFromOrNodeOperand(stmt, leftOperand, results);
            } catch (NonOptimizableOrClauseException e) {
                return;
            }

            try {
                extractFromOrNodeOperand(stmt, rightOperand, results);
            } catch(NonOptimizableOrClauseException e) {
                return;
            }
        } else if (node.getNodeType() == NodeTypes.IN_LIST_OPERATOR_NODE) {
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
     * extract routing-values from or-node operands, left and right side
     *
     * @param stmt the statement we operate on
     * @param operand the valueNode to extract values from, will recurse further into extracting from this node if necessary
     * @param results set of routing values, result-argument
     * @throws StandardException
     * @throws NonOptimizableOrClauseException if or-node operand is not optimizable, stop recursion
     */
    private void extractFromOrNodeOperand(ParsedStatement stmt, ValueNode operand, Set<String> results) throws StandardException, NonOptimizableOrClauseException {
        if (operand.getNodeType() == NodeTypes.OR_NODE || operand.getNodeType() == NodeTypes.IN_LIST_OPERATOR_NODE) {
            extractFromOrClauses(stmt, operand, results);
        } else {
            Object leftValue = extractPrimaryKeyValue(stmt, operand);
            if (leftValue != null) {
                results.add(leftValue.toString());
            } else {
                results.clear(); // cannot use routing
                throw new NonOptimizableOrClauseException();
            }
        }
    }


    /**
     * If a primary_key equals a constant value in the given node, and also is defined as the
     * routing key we return the constant value this statement asks for.
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

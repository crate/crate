package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.BinaryRelationalOperatorNode;
import org.cratedb.sql.parser.parser.ColumnReference;
import org.cratedb.sql.parser.parser.ValueNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class QueryPlanner {

    public static final String PRIMARY_KEY_VALUE = "primaryKeyValue";

    public static final String SETTINGS_OPTIMIZE_PK_QUERIES = "crate.planner.optimize_pk_queries";

    private Settings settings;

    @Inject
    public QueryPlanner(Settings settings) {
        this.settings = settings;
    }

    /**
     * Check if we can optimize queries based on the SQL WHERE clause.
     * Returns true if Visitor/Generator should stop operating, otherwise false.
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

        if (checkSinglePrimaryAndRouting(stmt, node)) {
            return true;
        }

        return false;
    }

    /**
     * If a primary_key equals a constant value in the given node, and also is defined as the
     * routing key, write the value to the {@link ParsedStatement} and return true.
     * The {@link org.cratedb.action.sql.TransportSQLAction} can then make decisions using
     * this values if e.g. a {@link org.elasticsearch.action.get.GetRequest} should be used
     * instead of a {@link org.elasticsearch.action.search.SearchRequest}.
     *
     * @param stmt
     * @param node
     * @return
     * @throws StandardException
     */
    private Boolean checkSinglePrimaryAndRouting(ParsedStatement stmt, ValueNode node)
            throws StandardException {
        List<String> primaryKeys = stmt.tableContext().primaryKeysIncludingDefault();
        if (node instanceof BinaryRelationalOperatorNode && ((BinaryRelationalOperatorNode)node).getOperatorType() == XContentGenerator.SQLOperatorTypes.EQUALS) {
            ValueNode leftOperand = ((BinaryRelationalOperatorNode)node).getLeftOperand();
            ValueNode rightOperand = ((BinaryRelationalOperatorNode)node).getRightOperand();
            Object value = null;
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
            if (value != null) {
                stmt.setPlannerResult(PRIMARY_KEY_VALUE, value.toString());
                return true;
            }
        }

        return false;
    }

}

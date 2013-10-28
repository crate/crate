package org.cratedb.action.parser;

import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.HashSet;

public class QueryPlanner {

    public static final String SETTINGS_OPTIMIZE_PK_QUERIES = "crate.planner.optimize_pk_queries";

    private Settings settings;

    @Inject
    public QueryPlanner(Settings settings) {
        this.settings = settings;
    }

    private boolean optimizePrimaryKeyQueries() {
        return settings.getAsBoolean(SETTINGS_OPTIMIZE_PK_QUERIES, true);
    }

    /**
     * this sets the {@link org.cratedb.action.sql.ParsedStatement#type()} of the ParsedStatement
     * E.g.: if the query contains something like pk_col = 1 in the where clause
     * this will turn a SEARCH_ACTION into a GET_ACTION
     * @param stmt
     */
    public void finalizeWhereClause(ParsedStatement stmt) {
        if (optimizePrimaryKeyQueries()) {
            finalizeMultiGet(stmt);

            if (stmt.primaryKeyLookupValue != null) {
                switch (stmt.nodeType()) {
                    case NodeTypes.CURSOR_NODE:
                        stmt.type(ParsedStatement.ActionType.GET_ACTION);
                        break;
                    case NodeTypes.UPDATE_NODE:
                        // version handling works only with sql facet and search_action
                        if (stmt.versionFilter == null) {
                            stmt.type(ParsedStatement.ActionType.UPDATE_ACTION);
                        }
                        break;
                    case NodeTypes.DELETE_NODE:
                        stmt.type(ParsedStatement.ActionType.DELETE_ACTION);
                        break;
                }
            }
        }
    }

    /**
     * will check the column for possible optimizations and if it contains any write the
     * appropriate values onto {@link ParsedStatement}.
     *
     * This should be called on each BinaryRelationalOperatorNode within the WhereClause.
     * In case the column should be skipped in the resulting query this method returns true.
     *
     * @param tableContext
     * @param stmt
     * @param parentNode
     * @param operator
     * @param columnName
     * @param value
     * @return true if the column should be omitted from the resulting lucene/xcontent query.
     */
    public boolean checkColumn(ITableExecutionContext tableContext, ParsedStatement stmt,
                            ValueNode parentNode, int operator,
                            String columnName, Object value) {
        if (!optimizePrimaryKeyQueries()) {
            if (columnName.equalsIgnoreCase("_version")) {
                raiseUnsupportedVersionSysColFilter();
            }

            return false;
        }

        if (operator != BinaryRelationalOperatorNode.EQUALS_RELOP) {
            stmt.columnsWithFilter.add(columnName);
            stmt.primaryKeyLookupValue = null;

            return false;
        }

        if (columnName.equalsIgnoreCase("_version")) {
            if (parentNode.getNodeType() == NodeTypes.AND_NODE) {
                if (stmt.versionFilter != null) {
                    throw new SQLParseException("Multiple _version columns in where clause are not supported");
                }
                stmt.versionFilter = ((Number)value).longValue();
                return true;
            } else {
                raiseUnsupportedVersionSysColFilter();
            }
        } else if (tableContext.primaryKeysIncludingDefault().contains(columnName)) {
            if (parentNode == null) {
                assert stmt.primaryKeyLookupValue == null;
                stmt.primaryKeyLookupValue = value.toString();
            } else if (parentNode.getNodeType() == NodeTypes.OR_NODE) {

                if (stmt.primaryKeyValues == null) {
                    stmt.primaryKeyValues = new HashSet<>();
                }
                stmt.primaryKeyValues.add(value.toString());
            } else if (stmt.columnsWithFilter.isEmpty()) {
                stmt.primaryKeyLookupValue = value.toString();
            }
        } else {
            stmt.columnsWithFilter.add(columnName);
            stmt.primaryKeyLookupValue = null;
        }

        return false;
    }

    private void raiseUnsupportedVersionSysColFilter() {
        throw new SQLParseException(
            "_version is only valid in the WHERE clause if paired with a single primary key column " +
            "and crate.planner.optimize.pk_queries enabled");
    }

    /**
     * move primaryKeyValues to routingValues if MultiGetRequest is not possible
     * @param stmt the ParsedStatement to operate on
     */
    private void finalizeMultiGet(ParsedStatement stmt) {
        // only leave MULTIGET_PRIMARY_KEY_VALUES as is when we can make a MultiGetRequest

        if (stmt.primaryKeyValues != null) {
            if (stmt.nodeType() != NodeTypes.CURSOR_NODE
                || stmt.hasOrderBy()
                || stmt.hasGroupBy()
                || !stmt.columnsWithFilter.isEmpty())
            {
                stmt.routingValues = stmt.primaryKeyValues;
                stmt.primaryKeyValues = null;
            } else {
                stmt.type(ParsedStatement.ActionType.MULTI_GET_ACTION);
            }
        }
    }
}

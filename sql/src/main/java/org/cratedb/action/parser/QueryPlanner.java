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
        if (!optimizePrimaryKeyQueries()) {
            return;
        }

        verifyVersionSysColumn(stmt);
        verifyPrimaryKeyValues(stmt);
        verifyRoutingValues(stmt);
        optimizeActionType(stmt);
    }

    private void verifyPrimaryKeyValues(ParsedStatement stmt) {
        if (stmt.primaryKeyValues.isEmpty()) {
            return;
        }


        if (stmt.nodeType() != NodeTypes.CURSOR_NODE
            || stmt.hasOrderBy()
            || stmt.hasGroupBy()
            || (stmt.limit != null && stmt.limit < stmt.primaryKeyValues.size()))
        {
            stmt.routingValues = stmt.primaryKeyValues;
            stmt.primaryKeyValues = new HashSet<>();
        } else if (stmt.columnsWithFilter.isEmpty()) {
            stmt.type(ParsedStatement.ActionType.MULTI_GET_ACTION);
        } else {
            stmt.primaryKeyValues.clear();
        }
    }

    private void verifyRoutingValues(ParsedStatement stmt) {
        if (!stmt.routingValues.isEmpty() && stmt.orClauses > 0 && !stmt.columnsWithFilter.isEmpty()) {
            stmt.routingValues.clear();
        }
    }

    private void optimizeActionType(ParsedStatement stmt) {
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

    private void verifyVersionSysColumn(ParsedStatement stmt) {
        if (stmt.versionFilter != null
            && stmt.primaryKeyLookupValue == null
            && stmt.nodeType() != NodeTypes.UPDATE_NODE)
        {
            raiseUnsupportedVersionSysColFilter();
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
                            ValueNode parentNode, Integer operator,
                            String columnName, Object value) {
        if (!optimizePrimaryKeyQueries() || stmt.tableNameIsAlias) {
            if (columnName.equalsIgnoreCase("_version")) {
                raiseUnsupportedVersionSysColFilter();
            }

            return false;
        }

        if (parentNode != null && parentNode.getNodeType() == NodeTypes.OR_NODE) {
            stmt.orClauses++;
        }
        if (operator == null || operator != BinaryRelationalOperatorNode.EQUALS_RELOP) {
            stmt.columnsWithFilter.add(columnName);
            stmt.primaryKeyLookupValue = null;

            return false;
        }

        if (columnName.equalsIgnoreCase("_version")) {
            if (handleVersionSysColumn(stmt, parentNode, (Number) value)) {
                return true;
            }
        } else if (tableContext.primaryKeysIncludingDefault().contains(columnName)) {
            handlePrimaryKeyValue(stmt, parentNode, value);
        } else if (tableContext.isRouting(columnName) && parentNode != null && parentNode.getNodeType() != NodeTypes.OR_NODE) {
            stmt.routingValues.add(value.toString());
            stmt.columnsWithFilter.add(columnName);
            resetPrimaryKeyValue(stmt);
        } else {
            stmt.columnsWithFilter.add(columnName);
            resetPrimaryKeyValue(stmt);
        }

        return false;
    }

    private void handlePrimaryKeyValue(ParsedStatement stmt, ValueNode parentNode, Object value) {
        if (parentNode == null) {
            assert stmt.primaryKeyLookupValue == null;
            stmt.primaryKeyLookupValue = value.toString();
        } else if (parentNode.getNodeType() == NodeTypes.OR_NODE
            || parentNode.getNodeType() == NodeTypes.IN_LIST_OPERATOR_NODE)
        {
            stmt.primaryKeyValues.add(value.toString());
        } else if (stmt.columnsWithFilter.isEmpty()) {  // likely AndNode and other column is a _version column
            stmt.primaryKeyLookupValue = value.toString();
        } else {
            stmt.routingValues.add(value.toString());
        }
    }

    private boolean handleVersionSysColumn(ParsedStatement stmt, ValueNode parentNode, Number value) {
        if (parentNode != null && parentNode.getNodeType() == NodeTypes.AND_NODE) {
            if (stmt.versionFilter != null) {
                throw new SQLParseException("Multiple _version columns in where clause are not supported");
            }
            stmt.versionFilter = value.longValue();
            return true;
        } else {
            raiseUnsupportedVersionSysColFilter();
        }
        return false;
    }

    private void resetPrimaryKeyValue(ParsedStatement stmt) {
        if (stmt.primaryKeyLookupValue != null) {
            stmt.routingValues.add(stmt.primaryKeyLookupValue);
            stmt.primaryKeyLookupValue = null;
        }
    }

    private void raiseUnsupportedVersionSysColFilter() {
        throw new SQLParseException(
            "_version is only valid in the WHERE clause if paired with a single primary key column " +
            "and crate.planner.optimize.pk_queries enabled");
    }
}

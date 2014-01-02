package org.cratedb.action.parser.visitors;

import com.google.common.collect.Lists;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.Constants;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;

/**
 * The InsertVisitor is an implementation of the XContentVisitor interface.
 * It will build a XContent document from a SQL ``INSERT`` stmt, usable as a ``IndexRequest`` source.
 */
public class InsertVisitor extends BaseVisitor {

    private ESLogger logger = Loggers.getLogger(InsertVisitor.class);
    private List<String> columnNameList;
    private List<String> primaryKeys;

    public InsertVisitor(NodeExecutionContext context, ParsedStatement stmt,
                         Object[] args) throws StandardException {
        super(context, stmt, args);
    }

    @Override
    protected void visit(InsertNode node) throws StandardException {
        // For building the fields we need to know the targets column names.
        // This could be done while visiting the ``TableName`` and ``ColumnReference`` node, but our tree is not
        // ordered, means ``targetTableName`` and ``targetColumnList`` are maybe visited too late.
        // So we *must* resolve this values explicitly here.

        tableName(node.getTargetTableName());
        if (tableContext.tableIsAlias()) {
            throw new SQLParseException("Table alias not allowed in INSERT statement.");
        }
        ResultColumnList targetColumnList = node.getTargetColumnList();

        // Get column names from index if not defined by query
        // NOTE: returned column name list is alphabetic ordered!
        if (targetColumnList == null) {
            columnNameList = Lists.newArrayList(tableContext.allCols());
        } else {
            for (ResultColumn column : targetColumnList) {
                if (column.getReference().getNodeType() == NodeType.NESTED_COLUMN_REFERENCE) {
                    throw new SQLParseException("Nested Column Reference not allowes in INSERT " +
                            "statement");
                }
            }
            columnNameList = Arrays.asList(targetColumnList.getColumnNames());
        }

        primaryKeys = tableContext.primaryKeys();
        if (primaryKeys.size() > 1) {
            throw new SQLParseException("Multiple primary key columns are not supported!");
        }

        ResultSetNode resultSetNode = node.getResultSetNode();
        if (resultSetNode instanceof RowResultSetNode) {
            stmt.indexRequests = new ArrayList<>(1);
            visit((RowResultSetNode)resultSetNode, 0);
        } else {
            RowsResultSetNode rowsResultSetNode = (RowsResultSetNode)resultSetNode;
            RowResultSetNode[] rows = rowsResultSetNode.getRows().toArray(
                new RowResultSetNode[rowsResultSetNode.getRows().size()]);
            stmt.indexRequests = new ArrayList<>(rows.length);

            for (int i = 0; i < rows.length; i++) {
                visit(rows[i], i);
            }
        }

        if (stmt.indexRequests.size() > 1) {
            stmt.type(ParsedStatement.ActionType.BULK_ACTION);
        } else {
            stmt.type(ParsedStatement.ActionType.INSERT_ACTION);
        }
    }

    private void visit(RowResultSetNode node, int idx) throws StandardException {
        IndexRequest indexRequest = new IndexRequest(stmt.tableName(), Constants.DEFAULT_MAPPING_TYPE);
        indexRequest.create(true);

        Map<String, Object> source = new HashMap<String, Object>();
        ResultColumnList resultColumnList = node.getResultColumns();

        for (ResultColumn column : resultColumnList) {
            String columnName = columnNameList.get(resultColumnList.indexOf(column));
            Object value = mappedValueFromNode(columnName, column.getExpression());

            source.put(columnName, value);
            if (primaryKeys.contains(columnName)) {
                indexRequest.id(value.toString());
            }
        }

        if (primaryKeys.size() > 0 && indexRequest.id() == null) {
            throw new SQLParseException(
                "Primary key is required but is missing from the insert statement");
        }

        indexRequest.source(source);
        stmt.indexRequests.add(idx, indexRequest);
    }
}

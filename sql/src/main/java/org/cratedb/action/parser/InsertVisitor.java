package org.cratedb.action.parser;

import com.google.common.collect.Lists;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.*;

/**
 * The InsertVisitor is an implementation of the XContentVisitor interface.
 * It will build a XContent document from a SQL ``INSERT`` stmt, usable as a ``IndexRequest`` source.
 */
public class InsertVisitor extends XContentVisitor {

    private boolean stopTraverse;
    private List<String> columnNameList;
    private ESLogger logger = Loggers.getLogger(InsertVisitor.class);
    private List<String> primaryKeys;
    private IndexRequest[] indexRequests;

    public InsertVisitor(ParsedStatement stmt) throws StandardException {
        super(stmt);
        stopTraverse = false;
    }

    @Override
    public XContentBuilder getXContentBuilder() {
        throw new UnsupportedOperationException("Use indexRequests() go get the built requests.");
    }

    public IndexRequest[] indexRequests() {
        return indexRequests;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        QueryTreeNode treeNode = (QueryTreeNode)node;
        switch (treeNode.getNodeType()) {
            case NodeTypes.INSERT_NODE:
                stopTraverse = true;
                return visit((InsertNode)node);
            default:
                return node;
        }
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraverse;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    private Visitable visit(InsertNode node) throws StandardException {
        // For building the fields we need to know the targets column names.
        // This could be done while visiting the ``TableName`` and ``ColumnReference`` node, but our tree is not
        // ordered, means ``targetTableName`` and ``targetColumnList`` are maybe visited too late.
        // So we *must* resolve this values explicitly here.
        String tableName = node.getTargetTableName().getTableName();
        ResultColumnList targetColumnList = node.getTargetColumnList();

        stmt.addIndex(tableName);
        stmt.tableContext(stmt.context().tableContext(tableName));

        // Get column names from index if not defined by query
        // NOTE: returned column name list is alphabetic ordered!
        if (targetColumnList == null) {
            columnNameList = Lists.newArrayList(stmt.tableContext().allCols());
        } else {
            columnNameList = Arrays.asList(targetColumnList.getColumnNames());
        }

        if (stmt.tableContext() != null) {
            // no tableContext if dynamicMapping is enabled and mapping is missing
            primaryKeys = stmt.tableContext().primaryKeys();
            if (primaryKeys.size() > 1) {
                throw new SQLParseException("Multiple primary key columns are not supported!");
            }
        } else {
            primaryKeys = new ArrayList<String>(0);
        }

        ResultSetNode resultSetNode = node.getResultSetNode();
        if (resultSetNode instanceof RowResultSetNode) {
            indexRequests = new IndexRequest[1];
            visit((RowResultSetNode)resultSetNode, 0);
        } else {
            RowsResultSetNode rowsResultSetNode = (RowsResultSetNode)resultSetNode;
            RowResultSetNode[] rows = rowsResultSetNode.getRows().toArray(
                new RowResultSetNode[rowsResultSetNode.getRows().size()]);
            indexRequests = new IndexRequest[rows.length];

            for (int i = 0; i < rows.length; i++) {
                visit(rows[i], i);
            }
        }

        return node;
    }

    private Visitable visit(RowResultSetNode node, int idx) throws StandardException {
        indexRequests[idx] = new IndexRequest(stmt.indices().get(0), stmt.context().DEFAULT_TYPE);
        indexRequests[idx].create(true);

        Map<String, Object> source = new HashMap<String, Object>();
        ResultColumnList resultColumnList = node.getResultColumns();

        for (ResultColumn column : resultColumnList) {
            String columnName = columnNameList.get(resultColumnList.indexOf(column));
            Object value = evaluateValueNode(columnName, column.getExpression());

            source.put(columnName, value);
            if (primaryKeys.contains(columnName)) {
                indexRequests[idx].id(value.toString());
            }
        }

        if (primaryKeys.size() > 0 && indexRequests[idx].id() == null) {
            throw new SQLParseException(
                "Primary key is required but is missing from the insert statement");
        }

        indexRequests[idx].source(source);

        return node;
    }

    public boolean isBulk() {
        return indexRequests.length > 1;
    }
}

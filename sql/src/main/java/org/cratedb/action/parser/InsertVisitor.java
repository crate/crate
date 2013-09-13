package org.cratedb.action.parser;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import com.google.common.collect.Lists;
import org.cratedb.action.sql.NodeExecutionContext;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The InsertVisitor is an implementation of the XContentVisitor interface.
 * It will build a XContent document from a SQL ``INSERT`` stmt, usable as a ``IndexRequest`` source.
 */
public class InsertVisitor implements XContentVisitor {

    private List<String> indices;
    private XContentBuilder jsonBuilder;
    private boolean stopTraverse;
    private NodeExecutionContext executionContext;
    private NodeExecutionContext.TableExecutionContext tableContext;
    private List<String> columnNameList;
    private int columnIndex;
    private int rowCount;
    private int rowIndex;


    private final List<Tuple<String, String>> outputFields;

    public InsertVisitor(NodeExecutionContext nodeExecutionContext) {
        this.executionContext = nodeExecutionContext;
        indices = new ArrayList<String>();
        try {
            jsonBuilder = XContentFactory.jsonBuilder();
        } catch (IOException ex) {
        }
        outputFields = new ArrayList<Tuple<String, String>>();
        columnIndex = 0;
        stopTraverse = false;
        rowCount = 0;
        rowIndex = -1;
    }

    @Override
    public XContentBuilder getXContentBuilder() {
        return jsonBuilder;
    }

    @Override
    public List<String> getIndices() {
        return indices;
    }

    @Override
    public List<Tuple<String, String>> outputFields() {
        return outputFields;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        QueryTreeNode treeNode = (QueryTreeNode)node;
        switch (treeNode.getNodeType()) {
            case NodeTypes.INSERT_NODE:
                return visit((InsertNode)node);
            case NodeTypes.ROWS_RESULT_SET_NODE:
                return visit((RowsResultSetNode)node);
            case NodeTypes.ROW_RESULT_SET_NODE:
                return visit((RowResultSetNode)node);
            case NodeTypes.RESULT_COLUMN:
                return visit((ResultColumn)node);
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
        if (stopTraverse == true) {
            // Closing the XContent root object.
            try {
                jsonBuilder.endObject();
                if (rowCount > 0) {
                    jsonBuilder.flush();
                    jsonBuilder.stream().write('\n');
                }
            } catch (IOException e) {
            }
        }
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

        indices.add(tableName);
        tableContext = executionContext.tableContext(tableName);

        // Get column names from index if not defined by query
        // NOTE: returned column name list is alphabetic ordered!
        if (targetColumnList == null) {
            tableContext = executionContext.tableContext(tableName);
            columnNameList = Lists.newArrayList(tableContext.allCols());
        } else {
            columnNameList = Arrays.asList(targetColumnList.getColumnNames());
        }

        return node;
    }

    private Visitable visit(RowsResultSetNode node) throws StandardException {
        rowCount = node.getRows().size();

        return node;
    }

    private Visitable visit(RowResultSetNode node) throws StandardException {
        if (rowCount > 0) {
            // Multiple rows detected, generate XContent for bulk requests
            rowIndex++;
            if (rowIndex > 0) {
                try {
                    jsonBuilder = jsonBuilder.endObject();
                    jsonBuilder.flush();
                    jsonBuilder.stream().write('\n');
                    generateBulkHeader();
                    jsonBuilder = jsonBuilder.startObject();
                } catch (IOException e) {
                }
            } else {
                generateBulkHeader();
                try {
                    jsonBuilder.startObject();
                } catch (IOException e) {
                }
            }
        } else {
            try {
                jsonBuilder.startObject();
            } catch (IOException e) {
            }
        }
        return node;
    }


    private Visitable visit(ResultColumn node) throws StandardException {
        if (rowCount > 0 && rowIndex < 0) {
            return node;
        }
        if (node.getExpression() instanceof ConstantNode) {
            String name = columnNameList.get(columnIndex);
            generate((ConstantNode)node.getExpression(), name);

            if (columnIndex == columnNameList.size()-1) {
                // reset columnIndex
                columnIndex = 0;
                if (rowIndex == rowCount-1) {
                    stopTraverse = true;
                }
            }

            columnIndex++;
        }
        return node;
    }

    private void generate(ConstantNode node, String columnName) {
        try {
            jsonBuilder.field(columnName,
                    tableContext.mapper().mappers().name(columnName).mapper().value(node.getValue()));
        } catch (IOException ex) {
        }
    }

    private void generateBulkHeader() {
        try {
            jsonBuilder.startObject();
            jsonBuilder.startObject("index");
            jsonBuilder.field("_index", indices.get(0));
            jsonBuilder.field("_type", "default");
            jsonBuilder.endObject();
            jsonBuilder.endObject();
            jsonBuilder.flush();
            jsonBuilder.stream().write('\n');
        } catch (IOException e) {
        }
    }

    public boolean isBulk() {
        if (rowCount > 0) {
            return true;
        }
        return false;
    }

}

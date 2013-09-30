package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import com.google.common.collect.Lists;
import org.cratedb.action.sql.NodeExecutionContext;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The InsertVisitor is an implementation of the XContentVisitor interface.
 * It will build a XContent document from a SQL ``INSERT`` stmt, usable as a ``IndexRequest`` source.
 */
public class InsertVisitor implements XContentVisitor {

    private final ParsedStatement stmt;
    private XContentBuilder jsonBuilder;
    private boolean stopTraverse;
    private NodeExecutionContext.TableExecutionContext tableContext;
    private List<String> columnNameList;
    private int columnIndex;
    private int rowCount;
    private int rowIndex;
    private byte streamSeparator;
    private ESLogger logger = Loggers.getLogger(InsertVisitor.class);


    public InsertVisitor(ParsedStatement stmt) throws StandardException {
        this.stmt = stmt;
        try {
            jsonBuilder = XContentFactory.jsonBuilder();
        } catch (IOException e) {
            logger.error("Cannot create the jsonBuilder", e);
            throw new StandardException(e);
        }
        columnIndex = 0;
        stopTraverse = false;
        rowCount = 0;
        rowIndex = -1;
        streamSeparator = JsonXContent.jsonXContent.streamSeparator();
    }

    @Override
    public XContentBuilder getXContentBuilder() {
        return jsonBuilder;
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
        tableContext = stmt.context().tableContext(tableName);

        // Get column names from index if not defined by query
        // NOTE: returned column name list is alphabetic ordered!
        if (targetColumnList == null) {
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
        try {
            if (rowCount > 0) {
                // Multiple rows detected, generate XContent usable for a BulkRequest
                rowIndex++;
                if (rowIndex > 0) {
                    // NOT first row node, close object from the first row, write new header and open another object for
                    // the next values
                    jsonBuilder = jsonBuilder.endObject();
                    jsonBuilder.flush();
                    jsonBuilder.stream().write(streamSeparator);
                    generateBulkHeader();
                    jsonBuilder = jsonBuilder.startObject();
                } else {
                    // first row node, generate the bulk header and open a object for the values
                    generateBulkHeader();
                    jsonBuilder.startObject();
                }
            } else {
                // No multiple rows, simple start an object usable for a IndexRequest
                jsonBuilder.startObject();
            }
        } catch (IOException e) {
            logger.error("Error while writing json", e);
            throw new StandardException(e);
        }
        return node;
    }


    private Visitable visit(ResultColumn node) throws StandardException {
        if (rowCount > 0 && rowIndex < 0) {
            // multiple rows detected but ``ResultColumn`` not inside a ``RowsResultSetNode``, ignore column
            return node;
        }
        String name = columnNameList.get(columnIndex);
        Object value = null;

        if (node.getExpression() instanceof ConstantNode) {
            value = ((ConstantNode)node.getExpression()).getValue();
            generate(value, name);
        } else if (node.getExpression() instanceof ParameterNode) {
            Object[] args = stmt.args();
            if (args.length == 0) {
                throw new StandardException("Missing statement parameters");
            }
            int parameterNumber = ((ParameterNode)node.getExpression()).getParameterNumber();
            try {
                value = args[parameterNumber];
            } catch (IndexOutOfBoundsException e) {
                throw new StandardException("Statement parameter value not found");
            }

            generate(value, name, false);
        }

        columnIndex++;
        if (columnIndex == columnNameList.size()) {
            // reset columnIndex
            columnIndex = 0;
            if (rowIndex == rowCount-1) {
                // end of processing
                closeRootObject();
                stopTraverse = true;
            }
        }

        return node;
    }

    private void generate(Object value, String columnName) throws StandardException {
        generate(value, columnName, true);
    }

    private void generate(Object value, String columnName,
                          Boolean useMapper) throws StandardException {
        if (useMapper) {
            value = tableContext.mapper().mappers().name(columnName).mapper().value(value);
        }
        try {
            jsonBuilder.field(columnName, value);
        } catch (IOException e) {
            logger.error("Error while writing json", e);
            throw new StandardException(e);
        }
    }

    private void closeRootObject() throws StandardException {
        // Closing the XContent root object.
        try {
            jsonBuilder.endObject();
            if (rowCount > 0) {
                // Multiple rows detected, write required XContent stream separator
                jsonBuilder.flush();
                jsonBuilder.stream().write(streamSeparator);
            }
        } catch (IOException e) {
            logger.error("Error while writing json", e);
            throw new StandardException(e);
        }
    }

    private void generateBulkHeader() throws StandardException {
        try {
            jsonBuilder.startObject();
            jsonBuilder.startObject("index");
            jsonBuilder.field("_index", stmt.indices().get(0));
            jsonBuilder.field("_type", "default");
            jsonBuilder.endObject();
            jsonBuilder.endObject();
            jsonBuilder.flush();
            jsonBuilder.stream().write(streamSeparator);
        } catch (IOException e) {
            logger.error("Error while writing json", e);
            throw new StandardException(e);
        }
    }

    public boolean isBulk() {
        if (rowCount > 0) {
            return true;
        }
        return false;
    }
}

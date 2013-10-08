package org.cratedb.action.parser;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.plugin.SQLPlugin;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class is responsible for generating XContent from the SQL Syntax Tree.
 * Currently only Select statements are supported that are translated into
 * Elasticsearch Query-Json in XContentBuilder format.
 * <p/>
 * Starting point of this class is @{link #generate(CursorNode)}
 * <p/>
 * The generated XContent is then available via the XContentBuilder that can be accessed
 * using the @{link #getXContentBuilder()} method.
 */
public class XContentGenerator {

    private final ParsedStatement stmt;
    private XContentBuilder jsonBuilder;

    private NodeExecutionContext.TableExecutionContext tableContext;
    private boolean requireVersion = false;

    /**
     * The operators here are a redefinition of those defined in {@link
     * BinaryRelationalOperatorNode}
     * There they are not static so they can't be used in the rangeQueryOperatorMap further below.
     */
    static class SQLOperatorTypes {
        static final int EQUALS = 1;
        static final int NOT_EQUALS = 2;
        static final int GREATER_THAN = 3;
        static final int GREATER_EQUALS = 4;
        static final int LESS_THAN = 5;
        static final int LESS_EQUALS = 6;
    }

    private static Map<Integer, String> rangeQueryOperatorMap = new HashMap<Integer, String>() {{
        put(SQLOperatorTypes.GREATER_THAN, "gt");
        put(SQLOperatorTypes.GREATER_EQUALS, "gte");
        put(SQLOperatorTypes.LESS_THAN, "lt");
        put(SQLOperatorTypes.LESS_EQUALS, "lte");
    }};

    public XContentGenerator(ParsedStatement stmt) throws StandardException {
        this.stmt = stmt;
        try {
            jsonBuilder = XContentFactory.jsonBuilder().startObject();
        } catch (IOException ex) {
            throw new StandardException(ex);
        }
    }

    public void generate(DeleteNode node) throws IOException, StandardException {
        SelectNode selectNode = (SelectNode) node.getResultSetNode();
        generate(selectNode.getFromList());
        whereClause(selectNode.getWhereClause());
    }

    public void generate(UpdateNode node) throws IOException, StandardException {

        jsonBuilder.startObject("query");
        whereClause(((SelectNode) node.getResultSetNode()).getWhereClause());
        jsonBuilder.endObject();

        jsonBuilder.startObject("facets");
        jsonBuilder.startObject("sql");
        jsonBuilder.startObject("sql");

        jsonBuilder.field("stmt", stmt.stmt());
        if (stmt.hasArgs()) {
            jsonBuilder.field("args", stmt.args());
        }
        jsonBuilder.endObject();
        jsonBuilder.endObject();
        jsonBuilder.endObject();


    }

    public void generate(CursorNode node) throws IOException, StandardException {

        if (node.statementToString().equals("SELECT")) {
            if (node.getOrderByList() != null) {
                generate(node.getOrderByList());
            }

            generate((SelectNode) node.getResultSetNode());

            offsetClause(node.getOffsetClause());
            fetchFirstClause(node.getFetchFirstClause());
        } else {
            throw new SQLParseException("unsupported sql statement: " + node.statementToString());
        }
    }

    private void offsetClause(ValueNode node) throws IOException, StandardException {
        fieldFromParamNodeOrConstantNode(node, "from", null);
    }

    private void fetchFirstClause(ValueNode node) throws IOException, StandardException {
        fieldFromParamNodeOrConstantNode(node, "size", this.tableContext.indexSettings().getAsInt("crate.sql.default.limit", SQLPlugin.DEFAULT_SELECT_LIMIT));
    }

    private void fieldFromParamNodeOrConstantNode(ValueNode node, String fieldName, Object defaultValue)
            throws IOException, StandardException {
        if (node == null) {
            if (defaultValue != null) {
                jsonBuilder.field(fieldName, defaultValue);
            }
            return;
        }
        if (node.isParameterNode()) {
            jsonBuilder.field(fieldName, stmt.args()[((ParameterNode) node).getParameterNumber()]);
        } else {
            jsonBuilder.field(fieldName, ((ConstantNode) node).getValue());
        }
    }

    private void whereClause(ValueNode node) throws IOException, StandardException {
        if (node != null) {
            generate(node);
        } else {
            jsonBuilder.field("match_all", new HashMap());
        }
    }

    private void generate(SelectNode node) throws IOException, StandardException {
        jsonBuilder.startObject("query");
        generate(node.getFromList());

        whereClause(node.getWhereClause());
        jsonBuilder.endObject();

        generate(node.getResultColumns());

        // only include the version if it was explicitly selected.
        if (requireVersion) {
            jsonBuilder.field("version", true);
        }
    }

    private void generate(OrderByList node) throws IOException {
        jsonBuilder.startArray("sort");

        for (OrderByColumn column : node) {
            jsonBuilder.startObject()
                    .startObject(column.getExpression().getColumnName())
                    .field("order", column.isAscending() ? "asc" : "desc")
                    .field("ignore_unmapped", true)
                    .endObject()
                    .endObject();
        }

        jsonBuilder.endArray();
    }

    private void generate(FromList fromList) throws StandardException {
        if (fromList.size() != 1) {
            throw new SQLParseException(
                    "Only exactly one from table is allowed, got: " + fromList.size());
        }
        FromTable table = fromList.get(0);
        if (!(table instanceof FromBaseTable)) {
            throw new SQLParseException(
                    "From type " + table.getClass().getName() + " not supported");
        }
        String name = table.getTableName().getTableName();
        tableContext = stmt.context().tableContext(name);
        if (tableContext == null) {
            throw new SQLParseException("No table definition found for " + name);
        }
        stmt.addIndex(name);
    }

    private void generate(ResultColumnList columnList) throws IOException, StandardException {
        Set<String> fields = new LinkedHashSet<String>();

        if (columnList == null) {
            return;
        }
        for (ResultColumn column : columnList) {
            if (column instanceof AllResultColumn) {
                for (String name : tableContext.allCols()) {
                    stmt.addOutputField(name, name);
                    fields.add(name);
                }
                continue;
            }

            String columnName = column.getExpression().getColumnName();
            String columnAlias = column.getName();
            if (columnName == null) {
                // column is a constantValue (e.g. "select 1 from ...");
                String columnValue = "";
                if (column.getExpression() instanceof NumericConstantNode) {
                    columnValue = ((NumericConstantNode) column.getExpression()).getValue()
                            .toString();
                } else if (column.getExpression() instanceof CharConstantNode) {
                    columnValue = ((CharConstantNode) column.getExpression()).getValue()
                            .toString();
                }
                throw new SQLParseException(
                        "selecting constant values (select " + columnValue + " from ...) is not " +
                                "supported");
            }

            if (columnName.startsWith("_")) {
                // treat this as internal, so this is not a field
                if (columnName.equals("_version")) {
                    requireVersion = true;
                }
            } else if (column.getExpression() instanceof NestedColumnReference) {
                // resolve XContent input and SQL output path from nested column path nodes
                NestedColumnReference nestedColumnReference = (NestedColumnReference) column
                        .getExpression();

                if (nestedColumnReference.pathContainsNumeric()) {
                    throw new SQLParseException("Selecting nested column array indexes is not " +
                            "supported");
                }

                fields.add(nestedColumnReference.xcontentPathString());
                columnName = nestedColumnReference.xcontentPathString();
                if (columnAlias == column.getExpression().getColumnName()) {
                    // if no alias ("AS") is defined, use the SQL syntax for the output column
                    // name
                    columnAlias = nestedColumnReference.sqlPathString();
                }
            } else {
                // this should be a normal field which will also be extracted from the source for
                // us in the search hit, so it is always safe to add it to the fields
                fields.add(columnName);
            }

            stmt.addOutputField(columnAlias, columnName);
        }
        if (fields.size() > 0) {
            jsonBuilder.field("fields", fields);
        }
    }

    private void generate(Integer operatorType, ColumnReference left, Object right)
            throws IOException, StandardException {

        String columnName = left.getColumnName();
        if (left instanceof NestedColumnReference) {
            NestedColumnReference nestedColumnReference = (NestedColumnReference) left;
            if (nestedColumnReference.pathContainsNumeric()) {
                throw new SQLParseException("Filtering by nested column array indexes is not " +
                        "supported");
            }
            columnName = nestedColumnReference.xcontentPathString();
        }

        if (operatorType == SQLOperatorTypes.EQUALS) {
            jsonBuilder.startObject("term")
                .field(columnName, right)
                .endObject();
        } else if (rangeQueryOperatorMap.containsKey(operatorType)) {
            jsonBuilder.startObject("range")
                .startObject(columnName)
                .field(rangeQueryOperatorMap.get(operatorType), right)
                .endObject()
                .endObject();
        } else if (operatorType == SQLOperatorTypes.NOT_EQUALS) {
            jsonBuilder.startObject("bool")
                .startObject("must_not")
                .startObject("term").field(columnName, right).endObject()
                .endObject()
                .endObject();
        } else {
            throw new SQLParseException("Unhandled operator: " + operatorType.toString());
        }
    }

    private void generate(Integer operatorType, ColumnReference left, ConstantNode right)
            throws IOException, StandardException {
        // if an operator is added here the swapOperator method should also be extended.

        String columnName = left.getColumnName();
        if (left instanceof NestedColumnReference) {
            NestedColumnReference nestedColumnReference = (NestedColumnReference) left;
            if (nestedColumnReference.pathContainsNumeric()) {
                throw new SQLParseException("Filtering by nested column array index is not " +
                        "supported");
            }
            columnName = ((NestedColumnReference) left).xcontentPathString();
        }

        if (operatorType == SQLOperatorTypes.EQUALS) {
            jsonBuilder.startObject("term")
                    .field(columnName, right.getValue())
                    .endObject();
        } else if (rangeQueryOperatorMap.containsKey(operatorType)) {
            jsonBuilder.startObject("range")
                    .startObject(columnName)
                    .field(rangeQueryOperatorMap.get(operatorType), right.getValue())
                    .endObject()
                    .endObject();
        } else if (operatorType == SQLOperatorTypes.NOT_EQUALS) {
            jsonBuilder.startObject("bool")
                    .startObject("must_not")
                    .startObject("term").field(columnName, right.getValue()).endObject()
                    .endObject()
                    .endObject();
        } else {
            throw new SQLParseException("Unhandled operator: " + operatorType.toString());
        }
    }

    /**
     * if the fieldName is on the right side and the value on the left the operator needs to be
     * switched
     * E.g.
     * 4 < pos
     * is translated to
     * pos > 4
     *
     * @param operatorType
     * @return the swapped operator
     */
    private Integer swapOperator(Integer operatorType) {

        switch (operatorType) {
            case SQLOperatorTypes.GREATER_THAN:
                return SQLOperatorTypes.LESS_THAN;
            case SQLOperatorTypes.GREATER_EQUALS:
                return SQLOperatorTypes.LESS_EQUALS;
            case SQLOperatorTypes.LESS_THAN:
                return SQLOperatorTypes.GREATER_THAN;
            case SQLOperatorTypes.LESS_EQUALS:
                return SQLOperatorTypes.GREATER_EQUALS;
            case SQLOperatorTypes.EQUALS:
                return operatorType;
            case SQLOperatorTypes.NOT_EQUALS:
                return operatorType;
            default:
                throw new SQLParseException("unsupported operator " + operatorType.toString());
        }
    }

    private void generate(Integer operatorType, ValueNode left, ValueNode right)
            throws IOException, StandardException {
        if (left instanceof ColumnReference
                && (right instanceof NumericConstantNode || right instanceof CharConstantNode)) {
            generate(operatorType, (ColumnReference) left, (ConstantNode) right);
            return;
        } else if ((left instanceof NumericConstantNode || left instanceof CharConstantNode)
                && right instanceof ColumnReference) {
            generate(swapOperator(operatorType), (ColumnReference) right, (ConstantNode) left);
            return;
        } else if (left instanceof ColumnReference && (right instanceof ParameterNode)) {
            generate(operatorType, (ColumnReference)left, stmt.args()[((ParameterNode) right).getParameterNumber()]);
            return;
        }

        generate(left);
        generate(right);
    }

    private void generate(BinaryRelationalOperatorNode node)
            throws IOException, StandardException {
        generate(node.getOperatorType(), node.getLeftOperand(), node.getRightOperand());
    }

    private void generate(OrNode node) throws IOException, StandardException {
        jsonBuilder.startObject("bool")
                .field("minimum_should_match", 1)
                .startArray("should");

        jsonBuilder.startObject();
        generate(node.getLeftOperand());
        jsonBuilder.endObject();
        jsonBuilder.startObject();
        generate(node.getRightOperand());
        jsonBuilder.endObject();

        jsonBuilder.endArray().endObject();
    }

    private void generate(AndNode node) throws IOException, StandardException {
        jsonBuilder.startObject("bool")
                .field("minimum_should_match", 1)
                .startArray("must");

        jsonBuilder.startObject();
        generate(node.getLeftOperand());
        jsonBuilder.endObject();
        jsonBuilder.startObject();
        generate(node.getRightOperand());
        jsonBuilder.endObject();

        jsonBuilder.endArray().endObject();
    }

    private void generate(IsNullNode node) throws IOException, StandardException {
        jsonBuilder
                .startObject("filtered")
                .startObject("filter")
                .startObject("missing")
                .field("field", node.getOperand().getColumnName())
                .field("existence", true)
                .field("null_value", true)
                .endObject()
                .endObject()
                .endObject();
    }

    private void generate(NotNode node) throws IOException, StandardException {
        jsonBuilder.startObject("bool").startObject("must_not");
        generate(node.getOperand());
        jsonBuilder.endObject().endObject();
    }

    private void generate(ValueNode node) throws IOException, StandardException {

        if (node instanceof BinaryRelationalOperatorNode) {
            generate((BinaryRelationalOperatorNode) node);
        } else if (node instanceof IsNullNode) {
            generate((IsNullNode) node);
        } else if (node instanceof NotNode) {
            generate((NotNode) node);
        } else if (node.getNodeType() == NodeTypes.AND_NODE) {
            generate((AndNode) node);
        } else if (node.getNodeType() == NodeTypes.OR_NODE) {
            generate((OrNode) node);
        } else {
            throw new SQLParseException("Unhandled node " + node.toString());
        }
    }

    public XContentBuilder getXContentBuilder() throws StandardException {
        return jsonBuilder;
    }


}

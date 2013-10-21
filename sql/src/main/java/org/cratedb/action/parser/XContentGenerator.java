package org.cratedb.action.parser;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.AggExprFactory;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

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

    public static final int DEFAULT_SELECT_LIMIT = 1000;

    private final ParsedStatement stmt;
    private XContentBuilder jsonBuilder;

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
            generate((SelectNode) node.getResultSetNode());

            if (node.getOrderByList() != null) {
                generate(node.getOrderByList());
            }

            if (stmt.hasGroupBy()) {
                stmt.offset = (Integer)getValueFromNode(node.getOffsetClause());
                stmt.limit = (Integer)getValueFromNode(node.getFetchFirstClause());
            } else {
                Object offset = getValueFromNode(node.getOffsetClause());
                if (offset != null) {
                    jsonBuilder.field("from", offset);
                }
                Object size = getValueFromNode(node.getFetchFirstClause());
                jsonBuilder.field("size", size != null ? size : DEFAULT_SELECT_LIMIT);
            }
        } else {
            throw new SQLParseException("unsupported sql statement: " + node.statementToString());
        }
    }

    private Object getValueFromNode(ValueNode node) {
        if (node == null) {
            return null;
        }

        if (node.isParameterNode()) {
            return stmt.args()[((ParameterNode)node).getParameterNumber()];
        }

        return ((ConstantNode)node).getValue();
    }

    private void whereClause(ValueNode node) throws IOException, StandardException {
        if (node != null) {
            if (stmt.context().queryPlanner().optimizedWhereClause(stmt, node)) {
                // stop further generation because we'll switch to Get/Update/DeleteRequest
                // instead of Search/DeleteByQueryRequest if planner checks succeeded
                return;
            }
            generate(node);
        } else {
            jsonBuilder.field("match_all", new HashMap<>());
        }
    }

    private void generate(SelectNode node) throws IOException, StandardException {
        if (node.getGroupByList() != null) {
            stmt.groupByColumnNames = new ArrayList<>(node.getGroupByList().size());

            for (GroupByColumn groupByColumn : node.getGroupByList()) {
                if (groupByColumn.getColumnExpression().getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE) {
                    stmt.groupByColumnNames.add(
                        ((NestedColumnReference) groupByColumn.getColumnExpression()).xcontentPathString());
                } else {
                    stmt.groupByColumnNames.add(groupByColumn.getColumnName());
                }
            }
        }

        generate(node.getFromList());
        generate(node.getResultColumns());

        if (stmt.countRequest()) {
            whereClause(node.getWhereClause());
        } else {
            jsonBuilder.startObject("query");
            whereClause(node.getWhereClause());
            jsonBuilder.endObject();
        }

        // only include the version if it was explicitly selected.
        if (requireVersion) {
            jsonBuilder.field("version", true);
        }
    }

    private void generate(OrderByList node) throws IOException, StandardException {
        if (stmt.hasGroupBy()) {
            stmt.orderByIndices = newArrayList();
            int idx;
            for (OrderByColumn column : node) {
                if (column.getExpression() instanceof AggregateNode) {
                    AggExpr aggExpr = AggExprFactory.createAggExpr(
                        ((AggregateNode) column.getExpression()).getAggregateName());

                    idx = stmt.resultColumnList.indexOf(aggExpr);
                } else {
                    ColumnReferenceDescription colrefDesc = null;
                    ValueNode columnExpression = column.getExpression();
                    if (columnExpression.getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE) {
                        colrefDesc = new ColumnReferenceDescription(
                            ((NestedColumnReference) columnExpression).xcontentPathString());
                    } else {
                         colrefDesc = new ColumnReferenceDescription(columnExpression.getColumnName());
                    }

                    idx = stmt.resultColumnList.indexOf(colrefDesc);
                }

                if (idx < 0) {
                    throw new SQLParseException(
                        "column in order by is also required in the result column list"
                    );
                }

                stmt.orderByIndices.add(new OrderByColumnIdx(idx, column.isAscending()));
            }
            return;
        }

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
        stmt.addIndex(name);
    }

    private void generate(ResultColumnList columnList) throws IOException, StandardException {
        if (columnList == null) {
            return;
        }
        Set<String> fields = new LinkedHashSet<>();

        if (stmt.hasGroupBy()) {
            stmt.resultColumnList = new ArrayList<>(columnList.size());
        }

        for (ResultColumn column : columnList) {
            if (column instanceof AllResultColumn) {
                if (stmt.hasGroupBy()) {
                    throw new SQLParseException(
                        "select * with group by not allowed. It is required to specify the columns explicitly");
                }
                for (String name : stmt.tableContextSafe().allCols()) {
                    stmt.addOutputField(name, name);
                    fields.add(name);
                }
                continue;
            }

            String columnName = column.getExpression().getColumnName();
            String columnAlias = column.getName();
            if (columnName == null) {
                if (column.getExpression() instanceof AggregateNode) {
                    handleAggregateNode(stmt, column);
                    continue;
                } else {
                    raiseUnsupportedSelectFromConstantNode(column);
                }
            } else if (columnName.startsWith("_")) {
                // treat this as internal, so this is not a field
                if (columnName.equals("_version")) {
                    requireVersion = true;
                }
            } else if (column.getExpression().getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE) {
                // resolve XContent input and SQL output path from nested column path nodes
                NestedColumnReference nestedColumnReference = (NestedColumnReference) column
                        .getExpression();

                if (nestedColumnReference.pathContainsNumeric()) {
                    throw new SQLParseException("Selecting nested column array indexes is not " +
                            "supported");
                }

                fields.add(nestedColumnReference.xcontentPathString());
                columnName = nestedColumnReference.xcontentPathString();
                if (columnAlias.equals(column.getExpression().getColumnName())) {
                    // if no alias ("AS") is defined, use the SQL syntax for the output column
                    // name
                    columnAlias = nestedColumnReference.sqlPathString();
                }

                if (stmt.hasGroupBy()) {
                    stmt.resultColumnList.add(new ColumnReferenceDescription(columnName));
                }
            } else {
                // this should be a normal field which will also be extracted from the source for
                // us in the search hit, so it is always safe to add it to the fields
                fields.add(columnName);

                if (stmt.hasGroupBy()) {
                    stmt.resultColumnList.add(new ColumnReferenceDescription(columnName));
                }
            }

            stmt.addOutputField(columnAlias, columnName);
        }

        /**
         * In case of GroupBy the {@link org.cratedb.action.groupby.SQLGroupingCollector}
         * handles the field lookup
         *
         * only the "query" key of the generated XContent can be parsed by the parser used in
         * {@link org.cratedb.action.SQLQueryService}
         */
        if (fields.size() > 0 && !stmt.hasGroupBy()) {
            jsonBuilder.field("fields", fields);
        }
    }

    private void handleAggregateNode(ParsedStatement stmt, ResultColumn column) {

        AggregateNode node = (AggregateNode)column.getExpression();
        if (node.getAggregateName().equals("COUNT(*)")) {
            if (stmt.hasGroupBy()) {
                stmt.resultColumnList.add(AggExprFactory.createAggExpr(node.getAggregateName()));
            }
            String alias = column.getName() != null ? column.getName() : node.getAggregateName();
            stmt.countRequest(true);
            stmt.addOutputField(alias, node.getAggregateName());
        } else {
            throw new SQLParseException("Unsupported Aggregate function " + node.getAggregateName());
        }
    }

    private void raiseUnsupportedSelectFromConstantNode(ResultColumn column) {
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

    private void generate(Integer operatorType, ColumnReference left, Object right)
            throws IOException, StandardException {

        String columnName = left.getColumnName();
        if (left.getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE) {
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
        if (left.getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE) {
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
        if (left instanceof ColumnReference && right instanceof ConstantNode) {
            generate(operatorType, (ColumnReference) left, (ConstantNode) right);
            return;
        } else if (left instanceof ConstantNode && right instanceof ColumnReference) {
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

    private void generate(InListOperatorNode node) throws IOException, StandardException {
        RowConstructorNode leftNode = node.getLeftOperand();
        RowConstructorNode rightNodes = node.getRightOperandList();
        ValueNode column;
        try {
            column = leftNode.getNodeList().get(0);
        } catch(IndexOutOfBoundsException e) {
            throw new SQLParseException("Invalid IN clause");
        }
        if (column instanceof ColumnReference) {
            jsonBuilder.startObject("terms").startArray(column.getColumnName());
            for (ValueNode listNode : rightNodes.getNodeList()) {
                jsonBuilder.value(stmt.visitor().evaluateValueNode(column.getColumnName(), listNode));
            }
            jsonBuilder.endArray().endObject();
        } else {
            throw new SQLParseException("Invalid IN clause");
        }

    }

    private void generate(NotNode node) throws IOException, StandardException {
        jsonBuilder.startObject("bool").startObject("must_not");
        generate(node.getOperand());
        jsonBuilder.endObject().endObject();
    }

    public void generate(LikeEscapeOperatorNode node) throws IOException, StandardException {
        ValueNode tmp;
        ValueNode left = node.getReceiver();
        ValueNode right = node.getLeftOperand();

        if (left.getNodeType() != NodeTypes.COLUMN_REFERENCE) {
            tmp = left;
            left = right;
            right = tmp;
        }

        String like = ((ConstantNode)right).getValue().toString();
        like = like.replace("%", "*");
        like = like.replace("_", "?");
        jsonBuilder.startObject("wildcard").field(left.getColumnName(), like).endObject();
    }

    private void generate(ValueNode node) throws IOException, StandardException {
        if (node instanceof BinaryRelationalOperatorNode) {
            generate((BinaryRelationalOperatorNode) node);
            return;
        }

        switch (node.getNodeType()) {
            case NodeTypes.IS_NULL_NODE:
                generate((IsNullNode) node);
                break;
            case NodeTypes.IN_LIST_OPERATOR_NODE:
                generate((InListOperatorNode) node);
                break;
            case NodeTypes.NOT_NODE:
                generate((NotNode) node);
                break;
            case NodeTypes.AND_NODE:
                generate((AndNode) node);
                break;
            case NodeTypes.OR_NODE:
                generate((OrNode) node);
                break;
            case NodeTypes.LIKE_OPERATOR_NODE:
                generate((LikeEscapeOperatorNode)node);
                break;
            default:
                throw new SQLParseException("Unhandled node " + node.toString());
        }
    }

    public XContentBuilder getXContentBuilder() throws StandardException {
        return jsonBuilder;
    }


}

package org.cratedb.action.parser.visitors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggExprFactory;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;



public class QueryVisitor extends BaseVisitor implements Visitor {

    private XContentBuilder jsonBuilder;

    private Query rootQuery;
    private Stack<BooleanQuery> queryStack = new Stack<>();
    private Map<Integer, String> rangeQueryOperatorMap = new HashMap<>();

    public QueryVisitor(NodeExecutionContext context, ParsedStatement stmt, Object[] args)
        throws SQLParseException
    {
        super(context, stmt, args);

        rangeQueryOperatorMap.put(BinaryRelationalOperatorNode.GREATER_THAN_RELOP, "gt");
        rangeQueryOperatorMap.put(BinaryRelationalOperatorNode.GREATER_EQUALS_RELOP, "gte");
        rangeQueryOperatorMap.put(BinaryRelationalOperatorNode.LESS_THAN_RELOP, "lt");
        rangeQueryOperatorMap.put(BinaryRelationalOperatorNode.LESS_EQUALS_RELOP, "lte");

        try {
            jsonBuilder = XContentFactory.jsonBuilder().startObject();
        } catch (IOException ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }
    }

    @Override
    public void visit(UpdateNode node) throws Exception {
        tableName(node.getTargetTableName());
        xcontent(node);

        Map<String, Object> updateDoc = new HashMap<>();
        for (ResultColumn rc: (node.getResultSetNode()).getResultColumns()){
            String columnName = rc.getName();
            if (rc.getReference() != null && rc.getReference() instanceof NestedColumnReference) {
                NestedColumnReference nestedColumn = (NestedColumnReference)rc.getReference();
                columnName = nestedColumn.xcontentPathString();
            }
            updateDoc.put(columnName, mappedValueFromNode(columnName, rc.getExpression()));
        }

        stmt.updateDoc(updateDoc);

        // optimization to UPDATE_ACTION is done in #afterVisit()
        stmt.type(ParsedStatement.ActionType.SEARCH_ACTION);
    }

    @Override
    protected void afterVisit() throws SQLParseException {
        super.afterVisit();
        stmt.query = rootQuery;
        stmt.xcontent = jsonBuilder.bytes();

        queryPlanner.finalizeWhereClause(stmt);
    }

    private void xcontent(UpdateNode node) throws Exception {

        jsonBuilder.startObject("query");
        whereClause(((SelectNode) node.getResultSetNode()).getWhereClause());
        jsonBuilder.endObject();

        // only include the version if it was explicitly selected.
        if (stmt.versionSysColumnSelected) {
            jsonBuilder.field("version", true);
        }

        jsonBuilder.startObject("facets");
        jsonBuilder.startObject("sql");
        jsonBuilder.startObject("sql");

        jsonBuilder.field("stmt", stmt.stmt);
        if (args != null && args.length > 0) {
            jsonBuilder.field("args", args);
        }
        jsonBuilder.endObject();
        jsonBuilder.endObject();
        jsonBuilder.endObject();
    }

    @Override
    public void visit(CursorNode node) throws Exception {
        visit((SelectNode)node.getResultSetNode());

        if (node.getOrderByList() != null) {
            visit(node.getOrderByList());
        }

        stmt.offset = (Integer)valueFromNode(node.getOffsetClause());
        stmt.limit = (Integer)valueFromNode(node.getFetchFirstClause());

        if (!stmt.hasGroupBy()) {
            if (stmt.offset != null) {
                jsonBuilder.field("from", stmt.offset);
            }

            Integer limit;
            if (stmt.limit != null) {
                limit = stmt.limit;
            } else {
                limit = SQLParseService.DEFAULT_SELECT_LIMIT;
            }
            jsonBuilder.field("size", limit);
        }

        // if query can be optimized to GET or MULTI_GET its done in #afterVisit
        stmt.type(ParsedStatement.ActionType.SEARCH_ACTION);
    }

    public void visit(SelectNode node) throws Exception {
        if (node.getGroupByList() != null) {
            addGroupByColumns(node.getGroupByList());
        }

        visit(node.getFromList());
        visit(node.getResultColumns());

        if (stmt.countRequest()) {
            whereClause(node.getWhereClause());
        } else {
            jsonBuilder.startObject("query");
            whereClause(node.getWhereClause());
            jsonBuilder.endObject();
        }

        // only include the version if it was explicitly selected.
        if (stmt.versionSysColumnSelected) {
            jsonBuilder.field("version", true);
        }
    }

    private void whereClause(ValueNode node) throws Exception {
        if (node == null) {
            rootQuery = new MatchAllDocsQuery();
            jsonBuilder.field("match_all", new HashMap<>());
            return;
        }

        visit(null, node);
    }

    private void addGroupByColumns(GroupByList groupByList) {
        stmt.groupByColumnNames = new ArrayList<>(groupByList.size());

        for (GroupByColumn column : groupByList) {
            stmt.groupByColumnNames.add(column.getColumnExpression().getColumnName());
        }
    }

    private void visit(OrderByList node) throws IOException, StandardException {
        // TODO lucene ?

        if (stmt.hasGroupBy()) {
            stmt.orderByIndices = new ArrayList<>();
            int idx;
            for (OrderByColumn column : node) {
                if (column.getExpression() instanceof AggregateNode) {
                    AggExpr aggExpr = AggExprFactory.createAggExpr(
                        ((AggregateNode) column.getExpression()).getAggregateName());

                    idx = stmt.resultColumnList.indexOf(aggExpr);
                } else {
                    String columnName = column.getExpression().getColumnName();
                    ColumnReferenceDescription colrefDesc = new ColumnReferenceDescription(columnName);
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
            if (column.getExpression().getNodeType() == NodeTypes.MATCH_FUNCTION_NODE) {
                jsonBuilder.startObject()
                        .field("_score", column.isAscending() ? "asc" : "desc")
                        .endObject();
            } else {
                jsonBuilder.startObject()
                    .startObject(column.getExpression().getColumnName())
                    .field("order", column.isAscending() ? "asc" : "desc")
                    .field("ignore_unmapped", true)
                    .endObject()
                    .endObject();
            }
        }
        jsonBuilder.endArray();

        stmt.setHasOrderBy(true);
    }

    private void visit(ResultColumnList columnList) throws Exception {
        if (columnList == null) {
            return;
        }

        Set<String> fields = new LinkedHashSet<>();
        stmt.resultColumnList = new ArrayList<>(columnList.size());

        for (ResultColumn column : columnList) {
             if (column instanceof AllResultColumn) {
                if (stmt.hasGroupBy()) {
                    throw new SQLParseException(
                        "select * with group by not allowed. It is required to specify the columns explicitly");
                }
                 Iterable<String> cols = tableContext.allCols();
                 for (String name : cols) {
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
            } else if (columnName.equals("_version")) {
                stmt.versionSysColumnSelected = true;
            } else if (column.getExpression().getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE) {
                NestedColumnReference nestedColumnReference =
                    (NestedColumnReference) column.getExpression();

                if (columnAlias.equals(columnName)) {
                    columnAlias = nestedColumnReference.sqlPathString();
                }

                fields.add(columnName);
            } else {
                fields.add(columnName);
            }

            stmt.resultColumnList.add(new ColumnReferenceDescription(columnName));
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
            stmt.resultColumnList.add(AggExprFactory.createAggExpr(node.getAggregateName()));
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
            columnValue = ((NumericConstantNode) column.getExpression()).getValue().toString();
        } else if (column.getExpression() instanceof CharConstantNode) {
            columnValue = ((CharConstantNode) column.getExpression()).getValue().toString();
        }
        throw new SQLParseException(
            "selecting constant values (select " + columnValue + " from ...) is not supported");
    }

    public void visit(DeleteNode node) throws Exception {
        SelectNode selectNode = (SelectNode) node.getResultSetNode();
        visit(selectNode.getFromList());
        whereClause(selectNode.getWhereClause());

        stmt.type(ParsedStatement.ActionType.DELETE_BY_QUERY_ACTION);
        // optimization to DELETE_ACTION is done in #afterVisit() if possible.
    }


    @Override
    public void visit(ValueNode parentNode, BinaryRelationalOperatorNode node) throws IOException {
        if (parentNode == null) {
            rootQuery = queryFromNode(parentNode, node);
            return;
        }

        BooleanQuery parentQuery = queryStack.peek();
        parentQuery.add(
            queryFromNode(parentNode, node),
            isAndNode(parentNode) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD
        );
    }

    @Override
    public void visit(ValueNode parentNode, IsNullNode node) throws IOException {
        jsonBuilder
            .startObject("filtered").startObject("filter").startObject("missing")
            .field("field", node.getOperand().getColumnName())
            .field("existence", true)
            .field("null_value", true)
            .endObject().endObject().endObject();

        // TODO: lucene query?
    }

    @Override
    protected void visit(ValueNode parentNode, LikeEscapeOperatorNode node) throws Exception {
        ValueNode tmp;
        ValueNode left = node.getReceiver();
        ValueNode right = node.getLeftOperand();

        if (left.getNodeType() != NodeTypes.COLUMN_REFERENCE) {
            tmp = left;
            left = right;
            right = tmp;
        }

        String like = valueFromNode(right).toString();
        // lucene uses * and ? as wildcard characters
        // but via SQL they are used as % and _
        // here they are converted back.
        like = like.replaceAll("(?<!\\\\)\\*", "\\\\*");
        like = like.replaceAll("(?<!\\\\)%", "*");
        like = like.replaceAll("\\\\%", "%");

        like = like.replaceAll("(?<!\\\\)\\?", "\\\\?");
        like = like.replaceAll("(?<!\\\\)_", "?");
        like = like.replaceAll("\\\\_", "_");
        jsonBuilder.startObject("wildcard").field(left.getColumnName(), like).endObject();

        // TODO: lucene query
    }

    @Override
    protected void visit(ValueNode parentNode, InListOperatorNode node) throws Exception {
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
                queryPlanner.checkColumn(tableContext, stmt, node,
                    BinaryRelationalOperatorNode.EQUALS_RELOP, column.getColumnName(), valueFromNode(listNode));
                jsonBuilder.value( valueFromNode(listNode));
            }
            jsonBuilder.endArray().endObject();
        } else {
            throw new SQLParseException("Invalid IN clause");
        }
    }

    @Override
    public void visit(ValueNode parentNode, AndNode node) throws Exception {
        jsonBuilder.startObject("bool").field("minimum_should_match", 1).startArray("must");
        binaryLogicalOperatorNode(parentNode, node);
    }

    @Override
    public void visit(ValueNode parentNode, OrNode node) throws Exception {
        jsonBuilder.startObject("bool").field("minimum_should_match", 1).startArray("should");
        binaryLogicalOperatorNode(parentNode, node);
    }

    private void binaryLogicalOperatorNode(ValueNode parentNode,
                                           BinaryLogicalOperatorNode node) throws Exception {
        BooleanQuery query = newBoolNode(parentNode);
        queryStack.add(query);

        jsonBuilder.startObject();
        visit(node, node.getLeftOperand());
        jsonBuilder.endObject();

        jsonBuilder.startObject();
        visit(node, node.getRightOperand());
        jsonBuilder.endObject();

        queryStack.pop();
        jsonBuilder.endArray().endObject();
    }

    @Override
    protected void visit(ValueNode parentNode, NotNode node) throws Exception {
        jsonBuilder.startObject("bool").startObject("must_not");
        visit(parentNode, node.getOperand());
        jsonBuilder.endObject().endObject();

        // TODO lucene query
    }


    private boolean isAndNode(ValueNode node) {
        return node.getNodeType() == NodeTypes.AND_NODE;
    }

    private Query queryFromNode(ValueNode parentNode, BinaryRelationalOperatorNode node) throws IOException {
        int operator = node.getOperatorType();
        String columnName;
        Object value;

        if (node.getLeftOperand() instanceof ColumnReference) {
            columnName = node.getLeftOperand().getColumnName();
            value = valueFromNode(node.getRightOperand());
        } else {
            operator = swapOperator(operator);
            columnName = node.getRightOperand().getColumnName();
            value = valueFromNode(node.getLeftOperand());
        }

        if  (queryPlanner.checkColumn(tableContext, stmt, parentNode, operator, columnName, value)) {
            // _version column that shouldn't be included in the query
            // this is kind of like:
            //      where pk_col = 1 and 1 = 1
            jsonBuilder.field("match_all", new HashMap<>());
            return new MatchAllDocsQuery();
        }

        Object from = null;
        Object to = null;
        boolean includeLower = false;
        boolean includeUpper = false;

        switch (operator) {
            case BinaryRelationalOperatorNode.EQUALS_RELOP:
                jsonBuilder.startObject("term").field(columnName, value).endObject();
                return new TermQuery(new Term(columnName, BytesRefs.toBytesRef(value)));
            case BinaryRelationalOperatorNode.NOT_EQUALS_RELOP:
                jsonBuilder.startObject("bool").startObject("must_not")
                    .startObject("term").field(columnName, value).endObject()
                    .endObject().endObject();

                BooleanQuery matchAllAndNot = new BooleanQuery();
                BooleanQuery notQuery = new BooleanQuery();
                notQuery.add(
                    new TermQuery(new Term(columnName, BytesRefs.toBytesRef(value))),
                    BooleanClause.Occur.MUST_NOT
                );
                matchAllAndNot.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
                matchAllAndNot.add(notQuery, BooleanClause.Occur.MUST);
                return matchAllAndNot;
            case BinaryRelationalOperatorNode.LESS_THAN_RELOP:
                to = value;
                includeUpper = false;
                break;
            case BinaryRelationalOperatorNode.LESS_EQUALS_RELOP:
                to = value;
                includeUpper = true;
                break;
            case BinaryRelationalOperatorNode.GREATER_THAN_RELOP:
                from = value;
                includeLower = false;
                break;
            case BinaryRelationalOperatorNode.GREATER_EQUALS_RELOP:
                from = value;
                includeLower = true;
                break;
            default:
                throw new SQLParseException("Unhandled operator " + operator);
        }

        jsonBuilder.startObject("range")
            .startObject(columnName).field(rangeQueryOperatorMap.get(operator), value).endObject()
            .endObject();

        // TODO:
        // FieldMappers fieldMappers = documentFieldMappers.smartName(columnName);
        // if (fieldMappers != null) {
        //     return fieldMappers.mapper().rangeQuery(
        //         from, to, includeLower, includeUpper, null
        //     );
        // }
        return new TermRangeQuery(
            columnName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to),
            includeLower, includeUpper
        );
    }

    private BooleanQuery newBoolNode(ValueNode parentNode) {
        BooleanQuery query = new BooleanQuery();
        query.setMinimumNumberShouldMatch(1);

        if (rootQuery == null) {
            rootQuery = query;
        } else {
            BooleanQuery parentQuery = queryStack.peek();
            parentQuery.add(query,
                isAndNode(parentNode) ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD
            );
        }

        return query;
    }

    private int swapOperator(int operator) {
        switch (operator) {
            case BinaryRelationalOperatorNode.LESS_THAN_RELOP:
                return BinaryRelationalOperatorNode.GREATER_THAN_RELOP;
            case BinaryRelationalOperatorNode.LESS_EQUALS_RELOP:
                return BinaryRelationalOperatorNode.GREATER_EQUALS_RELOP;
            case BinaryRelationalOperatorNode.GREATER_THAN_RELOP:
                return BinaryRelationalOperatorNode.LESS_THAN_RELOP;
            case BinaryRelationalOperatorNode.GREATER_EQUALS_RELOP:
                return BinaryRelationalOperatorNode.LESS_EQUALS_RELOP;
            default:
                return operator;
        }
    }

    @Override
    public void visit(ValueNode parentNode, MatchFunctionNode node) throws Exception {
        ColumnReference columnReference = node.getColumnReference();
        String query = (String)valueFromNode(node.getQueryText());
        jsonBuilder.startObject("match")
                .field(columnReference.getColumnName(), query)
                .endObject();
    }

}

package org.cratedb.action.parser.visitors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggExprFactory;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.parser.ColumnDescription;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.OrderByColumnName;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.information_schema.InformationSchemaColumn;
import org.cratedb.information_schema.InformationSchemaTableExecutionContext;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.NotFilter;
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
        if (tableContext.tableIsAlias()) {
            throw new SQLParseException("Table alias not allowed in UPDATE statement.");
        }
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

        buildIdxMap();

        if (stmt.isInformationSchemaQuery()) {
            stmt.type(ParsedStatement.ActionType.INFORMATION_SCHEMA);
        } else {
            // only non-information schema queries can be optimized
            queryPlanner.finalizeWhereClause(stmt);
        }
    }

    /**
     * if it's a group by sql request {@link org.cratedb.action.groupby.GroupByRow}'s are built on
     * the mapper/reducer.
     * the idxMap is used to map resultColumnList idx to the groupByRows internal idx.
     *
     * E.g. ResultColumnList: [CountAggExpr, ColumnDesc(city), AvgAggExpr, ColumnDesc(country)]
     *      Group By: country, city
     *
     * maps to
     *
     * GroupByRow:
     *   GroupByKey [country, city]
     *   AggStates [Count, Avg]
     *
     * So that groupByRow.get(idxMap[0]) will return the CountAggState.
     */
    protected void buildIdxMap() {
        if (!stmt.hasGroupBy()) {
            return;
        }

        stmt.idxMap = new Integer[stmt.resultColumnList.size()];
        int aggIdx = 0;
        int idx = 0;
        for (ColumnDescription columnDescription : stmt.resultColumnList) {
            switch (columnDescription.type) {
                case ColumnDescription.Types.AGGREGATE_COLUMN:
                    stmt.idxMap[idx] = aggIdx + stmt.groupByColumnNames.size();
                    aggIdx++;
                    break;

                case ColumnDescription.Types.CONSTANT_COLUMN:
                    stmt.idxMap[idx] = stmt.groupByColumnNames.indexOf(
                        ((ColumnReferenceDescription)columnDescription).name);
                    break;
            }

            idx++;
        }
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
        visit((SelectNode) node.getResultSetNode());

        if (node.getOrderByList() != null) {
            visit(node.getOrderByList());
        }

        stmt.offset((Integer)valueFromNode(node.getOffsetClause()));
        stmt.limit((Integer)valueFromNode(node.getFetchFirstClause()));

        if (!stmt.hasGroupBy()) {
            if (stmt.offset() > 0) {
                jsonBuilder.field("from", stmt.offset());
            }

            jsonBuilder.field("size", stmt.limit());
        }

        // if query can be optimized to GET or MULTI_GET its done in #afterVisit
        stmt.type(ParsedStatement.ActionType.SEARCH_ACTION);
    }

    public void visit(SelectNode node) throws Exception {
        visit(node.getFromList());

        if (node.getGroupByList() != null) {
            addGroupByColumns(node.getGroupByList());
        }
        visit(node.getResultColumns());

        if (stmt.countRequest()) {
            whereClause(node.getWhereClause());
        } else {
            jsonBuilder.startObject("query");
            whereClause(node.getWhereClause());
            jsonBuilder.endObject();
            if (stmt.scoreMinimum != null) {
                jsonBuilder.field("min_score", stmt.scoreMinimum);
            }
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

        String columnName;
        for (GroupByColumn column : groupByList) {
            columnName = column.getColumnExpression().getColumnName();
            if (tableContext.isMultiValued(columnName))
            {
                throw new GroupByOnArrayUnsupportedException(columnName);
            }
            stmt.groupByColumnNames.add(column.getColumnExpression().getColumnName());
        }
    }

    private void visit(OrderByList node) throws IOException, StandardException {
        if (stmt.hasGroupBy()) {
            stmt.orderByIndices = new ArrayList<>();
            int idx = -1;
            for (OrderByColumn column : node) {

                if (column.getExpression().getNodeType() == NodeTypes.AGGREGATE_NODE) {

                    AggregateNode aggNode = (AggregateNode)column.getExpression();
                    AggExpr aggExpr = getAggregateExpression(aggNode);
                    if (aggExpr != null) {
                        idx = stmt.resultColumnList.indexOf(aggExpr);
                    }

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
        int count = 0;
        for (OrderByColumn column : node) {
            String columnName = column.getExpression().getColumnName();
            count++;
            // orderByColumns are used to query the InformationSchema
            stmt.orderByColumns.add(
                new OrderByColumnName(columnName, count, column.isAscending())
            );
            jsonBuilder.startObject()
                .startObject(columnName)
                .field("order", column.isAscending() ? "asc" : "desc")
                .field("ignore_unmapped", true)
                .endObject()
                .endObject();
        }
        jsonBuilder.endArray();
    }

    private void visit(ResultColumnList columnList) throws Exception {
        if (columnList == null) {
            return;
        }

        Set<String> fields = new LinkedHashSet<>();
        stmt.resultColumnList = new ArrayList<>(columnList.size());
        stmt.aggregateExpressions = new ArrayList<>();

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
            } else if (column.getExpression().getNodeType() == NodeTypes.SYSTEM_COLUMN_REFERENCE) {
                if (columnName.equalsIgnoreCase("_version")) {
                    stmt.versionSysColumnSelected = true;
                }
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

    /**
     * extract/build an AggExpr from an AggregateNode
     * @param node an instance of AggregateNode
     * @return an instance of AggExpr, will never be null
     * @throws if no AggExpr could be extracted, e.g. because no valid parameter was given
     */
    private AggExpr getAggregateExpression(AggregateNode node) throws SQLParseException {
        String aggregateName = node.getAggregateName();
        AggExpr aggExpr;

        AggFunction<?> aggFunction = context.availableAggFunctions().get(aggregateName);
        if (aggFunction == null) {
            throw new SQLParseException(String.format("Unknown aggregate function %s", aggregateName));
        }
        ValueNode operand = node.getOperand();

        if (aggregateName.startsWith(CountAggFunction.NAME)) {
            if (operand != null) {
                validateCountOperand(operand);
            }
            aggExpr = AggExprFactory.createAggExpr(aggregateName, operand == null ? null : operand.getColumnName());
        } else {
            if (operand != null) {
                // check columns
                ColumnDefinition columnDefinition = tableContext.getColumnDefinition(operand.getColumnName());
                if (aggFunction.supportedColumnTypes().contains(columnDefinition.dataType)) {
                    aggExpr = AggExprFactory.createAggExpr(aggregateName, operand.getColumnName());
                } else {
                    throw new SQLParseException(
                            String.format("Invalid column type '%s' for aggregate function %s",
                                    columnDefinition.dataType, aggregateName));
                }
            } else {
                throw new SQLParseException(String.format("Missing parameter for %s() function", aggregateName));
            }

        }
        return aggExpr;
    }

    private void handleAggregateNode(ParsedStatement stmt, ResultColumn column) {

        AggregateNode node = (AggregateNode)column.getExpression();
        AggExpr aggExpr = getAggregateExpression(node);

        if (aggExpr != null) {
            if (aggExpr.functionName.startsWith(CountAggFunction.NAME)) {
                stmt.countRequest(true);
            }
            stmt.resultColumnList.add(aggExpr);
            stmt.aggregateExpressions.add(aggExpr);
            String alias = aggExpr.toString();

            stmt.addOutputField(alias, node.getAggregateName());
        }
    }


    /**
     * verify that the operand in the count function translates to a count(*)
     * or a column reference referencing the primary key
     * because anything else isn't supported right now.
     *
     * @param operand
     */
    private void validateCountOperand(ValueNode operand) throws SQLParseException {
        switch (operand.getNodeType()) {
            case NodeTypes.PARAMETER_NODE:
                ParameterNode parameterNode = (ParameterNode)operand;
                Object value = args[parameterNode.getParameterNumber()];
                if (!value.equals("*")) {
                    throw new SQLParseException("'select count(?)' only works with '*' as parameter");
                }
                break;
            case NodeTypes.COLUMN_REFERENCE:
                if (!tableContext.primaryKeys().contains(operand.getColumnName())) {
                    throw new SQLParseException(
                        "select count(columnName) is currently only supported on primary key columns"
                    );
                }
                break;

            default:
                throw new SQLParseException(
                    "Got an unsupported argument to the count aggregate function");
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
        if (stmt.tableNameIsAlias) {
            throw new SQLParseException("Table alias not allowed in DELETE statement.");
        }
        whereClause(selectNode.getWhereClause());

        stmt.type(ParsedStatement.ActionType.DELETE_BY_QUERY_ACTION);
        // optimization to DELETE_ACTION is done in #afterVisit() if possible.
    }


    @Override
    public void visit(ValueNode parentNode, BinaryRelationalOperatorNode node) throws IOException {

        addToLuceneQueryStack(
            parentNode,
            queryFromBinaryRelationalOpNode(parentNode, node)
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

        if (stmt.isInformationSchemaQuery()) {
            addToLuceneQueryStack(
                parentNode,
                IsNullFilteredQuery(node.getOperand().getColumnName())
            );
        }
    }

    private Query IsNullFilteredQuery(String columnName) {
        InformationSchemaColumn column =
            ((InformationSchemaTableExecutionContext) tableContext).fieldMapper().get(columnName);
        // no filter if non-existing column
        if (column == null) {
            return new MatchAllDocsQuery();
        } else {
            Filter isNullFilter = new NotFilter(column.rangeFilter(null, null, true, true));
            return new FilteredQuery(new MatchAllDocsQuery(), isNullFilter);
        }

    }

    @Override
    protected void visit(ValueNode parentNode, LikeEscapeOperatorNode node) throws Exception {
        ValueNode tmp;
        ValueNode left = node.getReceiver();
        ValueNode right = node.getLeftOperand();

        if (left.getNodeType() != NodeTypes.COLUMN_REFERENCE
            && left.getNodeType() !=  NodeTypes.NESTED_COLUMN_REFERENCE) {
            tmp = left;
            left = right;
            right = tmp;
        }

        String columnName = left.getColumnName();
        String like = mappedValueFromNode(columnName, right).toString();

        queryPlanner.checkColumn(tableContext, stmt, parentNode, null, columnName, like);

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

        if (stmt.isInformationSchemaQuery()) {
            addToLuceneQueryStack(
                parentNode,
                new WildcardQuery(new Term(columnName, like))
            );
        }
    }

    private void addToLuceneQueryStack(ValueNode parentNode, Query query) {
        if (parentNode == null || rootQuery == null) {
            rootQuery = query;
            return;
        }

        BooleanQuery parentQuery = queryStack.peek();
        parentQuery.add(
            query,
            isOrNode(parentNode) ? BooleanClause.Occur.SHOULD : BooleanClause.Occur.MUST
        );
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
                String columnName = column.getColumnName();
                queryPlanner.checkColumn(tableContext, stmt, node,
                    BinaryRelationalOperatorNode.EQUALS_RELOP, columnName,
                        mappedValueFromNode(columnName, listNode));
                jsonBuilder.value( mappedValueFromNode(columnName, listNode));
            }
            jsonBuilder.endArray().endObject();
        } else {
            throw new SQLParseException("Invalid IN clause");
        }

        if (stmt.isInformationSchemaQuery()) {
            BooleanQuery query = new BooleanQuery();
            query.setMinimumNumberShouldMatch(1);

            for (ValueNode valueNode : rightNodes.getNodeList()) {

                query.add(
                    new TermQuery(new Term(
                        column.getColumnName(),
                        BytesRefs.toBytesRef(mappedValueFromNode(column.getColumnName(), valueNode)))
                    ),
                    BooleanClause.Occur.SHOULD
                );
            }

            addToLuceneQueryStack(parentNode, query);
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
        if (node.getNodeType() == NodeTypes.OR_NODE) {
            query.setMinimumNumberShouldMatch(1);
        }

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

        ValueNode parent = parentNode;
        if (stmt.isInformationSchemaQuery()) {
            BooleanQuery query = new BooleanQuery();
            BooleanQuery nestedQuery = new BooleanQuery();

            query.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            query.add(nestedQuery, BooleanClause.Occur.MUST_NOT);

            addToLuceneQueryStack(parentNode, query);
            queryStack.add(nestedQuery);

            // if this is beneath a AndNode or OrNode persist the correct parent
            // but if this is the first node the rootQuery shouldn't be overwritten
            // because the above BooleanQuery became the rootQuery.
            if (parent == null) {
                parent = node;
            }
        }

        visit(parent, node.getOperand());

        if (stmt.isInformationSchemaQuery()) {
            queryStack.pop();
        }

        jsonBuilder.endObject().endObject();

    }

    private boolean isOrNode(ValueNode node) {
        return node.getNodeType() == NodeTypes.OR_NODE;
    }

    private Query queryFromBinaryRelationalOpNode(ValueNode parentNode, BinaryRelationalOperatorNode node) throws IOException {
        int operator = node.getOperatorType();
        String columnName;
        Object value;

        if (node.getLeftOperand() instanceof ColumnReference) {
            columnName = node.getLeftOperand().getColumnName();
            value = mappedValueFromNode(columnName, node.getRightOperand());
        } else {
            operator = swapOperator(operator);
            columnName = node.getRightOperand().getColumnName();
            value = mappedValueFromNode(columnName, node.getLeftOperand());
        }

        // currently the lucene queries are only used for information schema queries.
        // therefore for non-information-schema-queries just the xcontent query is built.

        if (stmt.isInformationSchemaQuery()) {
            return buildLuceneQuery(operator, columnName, value);
        }

        if  (queryPlanner.checkColumn(tableContext, stmt, parentNode, operator, columnName, value)) {
            // _version column that shouldn't be included in the query
            // this is kind of like:
            //      where pk_col = 1 and 1 = 1
            jsonBuilder.field("match_all", new HashMap<>());
            return null;
        }

        if (columnName.equalsIgnoreCase("_score")) {
            if (operator != BinaryRelationalOperatorNode.GREATER_THAN_RELOP
                    && operator != BinaryRelationalOperatorNode.GREATER_EQUALS_RELOP) {
                throw new SQLParseException("Filtering by _score can only be done using a " +
                        "greater-than or greater-equals operator");
            }
            // type validated by SQLFieldMapper
            stmt.scoreMinimum = ((Number) value).doubleValue();
            jsonBuilder.field("match_all", new HashMap<>());
            return null;
        }

        switch (operator) {
            case BinaryRelationalOperatorNode.EQUALS_RELOP:
                jsonBuilder.startObject("term").field(columnName, value).endObject();
                break;
            case BinaryRelationalOperatorNode.NOT_EQUALS_RELOP:
                jsonBuilder.startObject("bool").startObject("must_not")
                    .startObject("term").field(columnName, value).endObject()
                    .endObject().endObject();
                break;
            case BinaryRelationalOperatorNode.LESS_THAN_RELOP:
            case BinaryRelationalOperatorNode.LESS_EQUALS_RELOP:
            case BinaryRelationalOperatorNode.GREATER_THAN_RELOP:
            case BinaryRelationalOperatorNode.GREATER_EQUALS_RELOP:
                jsonBuilder.startObject("range")
                    .startObject(columnName).field(rangeQueryOperatorMap.get(operator), value).endObject()
                    .endObject();
                break;
            default:
                throw new SQLParseException("Unhandled operator " + operator);
        }

        return null;
    }

    private Query buildLuceneQuery(int operator, String columnName, Object value) {
        Object from = null;
        Object to = null;
        boolean includeLower = false;
        boolean includeUpper = false;

        InformationSchemaColumn column =
            ((InformationSchemaTableExecutionContext)tableContext).fieldMapper().get(columnName);

        // if column does not exist - no docs match query
        if (column == null) {
            BooleanQuery noOpQuery = new BooleanQuery();
            noOpQuery.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST_NOT);
            return noOpQuery;
        }

        switch (operator) {
            case BinaryRelationalOperatorNode.EQUALS_RELOP:
                if (column.type == SortField.Type.STRING) {
                    return new TermQuery(new Term(columnName, value.toString()));
                } else {
                    return column.rangeQuery(value, value, true, true);
                }
            case BinaryRelationalOperatorNode.NOT_EQUALS_RELOP:
                BooleanQuery matchAllAndNot = new BooleanQuery();
                matchAllAndNot.add(
                    buildLuceneQuery(BinaryRelationalOperatorNode.EQUALS_RELOP, columnName, value),
                    BooleanClause.Occur.MUST_NOT
                );
                matchAllAndNot.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
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

        return column.rangeQuery(from, to, includeLower, includeUpper);
    }

    private BooleanQuery newBoolNode(ValueNode parentNode) {
        BooleanQuery query = new BooleanQuery();
        addToLuceneQueryStack(parentNode, query);

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
        if (stmt.isInformationSchemaQuery()) {
            addToLuceneQueryStack(
                    parentNode,
                    buildLuceneQuery(BinaryRelationalOperatorNode.EQUALS_RELOP,
                            columnReference.getColumnName(), mappedValueFromNode(
                            columnReference.getColumnName(),
                            node.getQueryText()))
            );
        }

        String query = (String)mappedValueFromNode(columnReference.getColumnName(),
                node.getQueryText());
        jsonBuilder.startObject("match")
                .field(columnReference.getColumnName(), query)
                .endObject();
    }

}

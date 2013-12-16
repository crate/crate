package org.cratedb.action.parser.visitors;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.any.AnyAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountColumnAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountDistinctAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountStarAggFunction;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.OrderByColumnName;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.lucene.fields.LuceneField;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.cratedb.sql.OrderByAmbiguousException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.cratedb.stats.ShardStatsTable;
import org.elasticsearch.common.collect.Tuple;
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

        if (stmt.isInformationSchemaQuery()) {
            stmt.type(ParsedStatement.ActionType.INFORMATION_SCHEMA);
        } else if (stmt.isStatsQuery()) {
            if (stmt.isGlobalAggregate()) {
                // enable reducers
                stmt.partialReducerCount = -1;
            }
            stmt.type(ParsedStatement.ActionType.STATS);
        } else {
            // only non-information schema queries can be optimized
            queryPlanner.finalizeWhereClause(stmt);
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

        stmt.offset((Integer) valueFromNode(node.getOffsetClause()));
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
        } else if (stmt.type() != null && stmt.type() == ParsedStatement.ActionType.STATS) {
            stmt.partialReducerCount = 0;
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
        List<String> columnNames = new ArrayList<>();
        List<String> aliases = new ArrayList<>();

        for (Tuple<String, String> column : stmt.outputFields()) {
            columnNames.add(column.v2());
            if (!column.v2().equals(column.v1())) {
                aliases.add(column.v1());
            } else {
                aliases.add(null);
            }
        }

        if (stmt.hasGroupBy() || stmt.isGlobalAggregate() || stmt.isStatsQuery()) {
            genOrderByIndices(node, columnNames, aliases);
            return;
        }

        genXContentOrderBy(node, columnNames, aliases);
    }

    private void genXContentOrderBy(OrderByList node, List<String> columnNames, List<String> aliases) throws IOException {
        int idxNames;
        int idxAliases;
        jsonBuilder.startArray("sort");
        int count = 0;
        for (OrderByColumn column : node) {
            String columnName = column.getExpression().getColumnName();
            idxNames = columnNames.indexOf(columnName);
            idxAliases = aliases.indexOf(columnName);
            if (idxNames > -1 && idxAliases > -1) {
                throw new OrderByAmbiguousException(columnName);
            } else if (idxAliases > -1 && idxNames < 0) {
                columnName = stmt.outputFields().get(idxAliases).v2();
            }

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

    private void genOrderByIndices(OrderByList node, List<String> columnNames, List<String> aliases) throws StandardException {
        int idxNames;
        int idxAliases;
        stmt.orderByIndices = new ArrayList<>();

        for (OrderByColumn column : node) {
            String columnName;
            if (column.getExpression().getNodeType() == NodeTypes.AGGREGATE_NODE) {
                AggExpr aggExpr = getAggregateExpression((AggregateNode)column.getExpression());
                columnName = aggExpr.toString();
            } else if (column.getExpression().getNodeType() == NodeTypes.NESTED_COLUMN_REFERENCE ){
                columnName = ((NestedColumnReference)column.getExpression()).sqlPathString();
            } else {
                columnName = column.getExpression().getColumnName();
            }
            idxNames = columnNames.indexOf(columnName);
            idxAliases = aliases.indexOf(columnName);

            if (idxNames > -1 && idxAliases > -1) {
                throw new OrderByAmbiguousException(columnName);
            } else if (idxNames < 0 && idxAliases < 0) {
                throw new SQLParseException(
                    "column in order by is also required in the result column list"
                );
            }

            stmt.orderByIndices.add(
                new OrderByColumnIdx(Math.max(idxNames, idxAliases), column.isAscending()));
        }
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


        if (!stmt.hasGroupBy()) {
            /**
             * In case of GroupBy the {@link org.cratedb.action.groupby.SQLGroupingCollector}
             * handles the field lookup
             *
             * only the "query" key of the generated XContent can be parsed by the parser used in
             * {@link org.cratedb.action.SQLQueryService}
             */
            if (fields.size() > 0) {
                jsonBuilder.field("fields", fields);
            }
            int aggExpressionsSize = stmt.aggregateExpressions().size();
            if (aggExpressionsSize > 0 && aggExpressionsSize < stmt.outputFields.size()) {
                throw new SQLParseException("Only aggregate expressions allowed here");
            }
        }


    }

    /**
     * extract/build an AggExpr from an AggregateNode
     * @param node an instance of AggregateNode
     * @return an instance of AggExpr, will never be null
     * @throws SQLParseException if no AggExpr could be extracted, e.g. because no valid parameter was given
     */
    private AggExpr getAggregateExpression(AggregateNode node) throws SQLParseException {
        String aggregateName = node.getAggregateName();

        AggFunction<?> aggFunction;
        if (aggregateName.equals(CountColumnAggFunction.NAME)) {
            if (node.isDistinct()) {
                aggregateName = CountDistinctAggFunction.NAME;
            } else if (node.getOperand().getNodeType() == NodeTypes.PARAMETER_NODE) {
                // COUNT(*) with parameter
                aggregateName = CountStarAggFunction.NAME;
            }
        }
        aggFunction = context.availableAggFunctions().get(aggregateName);


        if (aggFunction == null) {
            throw new SQLParseException(String.format("Unknown aggregate function %s", aggregateName));
        }

        if (node.isDistinct() && !aggFunction.supportsDistinct()) {
            throw new SQLParseException(
                String.format("The distinct keyword can't be used with the %s function", aggregateName));
        }

        ValueNode operand = node.getOperand();
        ParameterInfo parameterInfo = null;
        if (operand != null) {
            parameterInfo = resolveParameterInfo(operand, aggFunction);
        }

        return new AggExpr(aggregateName, parameterInfo, node.isDistinct());
    }

    private ParameterInfo resolveParameterInfo(ValueNode node, AggFunction aggFunction) {
        assert node != null;
        String columnName;

        switch (node.getNodeType()) {
            case NodeTypes.PARAMETER_NODE:
                // value of the parameterNode is a literal, Currently a literal is considered a "isAllColumn"
                return null;
            case NodeTypes.COLUMN_REFERENCE:
                columnName = node.getColumnName();
                break;
            case NodeTypes.NESTED_COLUMN_REFERENCE:
                columnName = node.getColumnName();
                break;
            default:
                throw new SQLParseException("Got an unsupported argument to a aggregate function");
        }

        // check columns
        ColumnDefinition columnDefinition = tableContext.getColumnDefinition(columnName);
        if (columnDefinition == null) {
            throw new SQLParseException(String.format("Unknown column '%s'", columnName));
        }
        if (!aggFunction.supportedColumnTypes().contains(columnDefinition.dataType)) {
            throw new SQLParseException(
                String.format("Invalid column type '%s' for aggregate function %s",
                    columnDefinition.dataType, aggFunction.name())
            );
        }

        return new ParameterInfo(columnName, columnDefinition.dataType);
    }

    private void handleAggregateNode(ParsedStatement stmt, ResultColumn column) {

        AggregateNode node = (AggregateNode)column.getExpression();
        AggExpr aggExpr = getAggregateExpression(node);

        if (aggExpr != null) {
            if (aggExpr.functionName.startsWith(CountStarAggFunction.NAME)) {
                stmt.hasCountStarAggregate(true);
            }
            stmt.resultColumnList.add(aggExpr);
            stmt.aggregateExpressions.add(aggExpr);
            if (aggExpr.isDistinct) {
                stmt.hasDistinctAggregate = true;
            }
            if (aggExpr.functionName.equals(AnyAggFunction.NAME)) {
                stmt.hasStoppableAggregate = true;
            }
            String columnName = aggExpr.toString();

            stmt.addOutputField(column.getName() != null ? column.getName() : columnName, columnName);
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

        if (stmt.isInformationSchemaQuery() || stmt.isStatsQuery()) {
            addToLuceneQueryStack(
                parentNode,
                IsNullFilteredQuery(node.getOperand().getColumnName())
            );
        }
    }

    private Query IsNullFilteredQuery(String columnName) {
        LuceneField column = tableContext.luceneFieldMapper().get(columnName);
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

        if (stmt.isInformationSchemaQuery() || stmt.isStatsQuery()) {
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

        if (stmt.isInformationSchemaQuery() || stmt.isStatsQuery()) {
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
        if (stmt.isInformationSchemaQuery() || stmt.isStatsQuery()) {
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

        if (stmt.isInformationSchemaQuery() || stmt.isStatsQuery()) {
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

        boolean plannerResult = queryPlanner.checkColumn(tableContext, stmt, parentNode,
                operator, columnName, value);

        // currently the lucene queries are only used for information schema queries.
        // therefore for non-information-schema-queries just the xcontent query is built.

        if (stmt.isInformationSchemaQuery()) {
            return buildLuceneQuery(operator, columnName, value);
        }
        if (stmt.isStatsQuery()) {
            if (columnName.equalsIgnoreCase(ShardStatsTable.Columns.TABLE_NAME)) {
                stmt.addIndex((String)value);
            }
            return buildLuceneQuery(operator, columnName, value);
        }

        if  (plannerResult) {
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

        LuceneField column = tableContext.luceneFieldMapper().get(columnName);

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
        if (stmt.isInformationSchemaQuery() || stmt.isStatsQuery()) {
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

        stmt.columnsWithFilter.add(columnReference.getColumnName());
    }

}

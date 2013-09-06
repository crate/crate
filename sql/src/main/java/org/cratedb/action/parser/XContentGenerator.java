package org.cratedb.action.parser;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentFieldMappers;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.service.IndexService;

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

    private List<String> indices;
    private XContentBuilder jsonBuilder;

    private final List<Tuple<String, String>> outputFields;

    private IndexMetaData indexMetaData;
    private MappingMetaData mapping;
    private NodeExecutionContext executionContext;
    private DocumentMapper mapper;
    private IndexService indexService;
    private DocumentFieldMappers fieldMappers;
    private DocumentMapper documentMapper;
    private NodeExecutionContext.TableExecutionContext tableContext;
    private boolean requireVersion = false;


    /**
     * returns the requested output fields as a list of tuples where
     * the left side is the alias and the right side is the column name
     *
     * @return list of tuples
     */
    public List<Tuple<String, String>> outputFields() {
        return outputFields;
    }

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

    @Inject
    public XContentGenerator(NodeExecutionContext executionContext) {
        this.executionContext = executionContext;
        indices = new ArrayList<String>();
        try {
            jsonBuilder = XContentFactory.jsonBuilder().startObject();
        } catch (IOException ex) {
        }
        outputFields = new ArrayList<Tuple<String, String>>();

    }

    public void generate(CursorNode node) throws IOException, StandardException {

        if (node.statementToString().equals("SELECT")) {
            if (node.getOrderByList() != null) {
                generate(node.getOrderByList());
            }

            generate((SelectNode) node.getResultSetNode());

            if (node.getOffsetClause() != null) {
                jsonBuilder.field("from",
                        ((NumericConstantNode) node.getOffsetClause()).getValue());
            }
            if (node.getFetchFirstClause() != null) {
                jsonBuilder.field("size",
                        ((NumericConstantNode) node.getFetchFirstClause()).getValue());
            }
        } else {
            throw new SQLParseException("unsupported sql statement: " + node.statementToString());
        }
    }

    private void generate(SelectNode node) throws IOException, StandardException {
        jsonBuilder.startObject("query");
        generate(node.getFromList());

        ValueNode whereClause = node.getWhereClause();
        if (whereClause != null) {
            switch (whereClause.getNodeType()) {
                case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
                    generate((BinaryRelationalOperatorNode) whereClause);
                    break;
                case NodeTypes.AND_NODE:
                    generate((AndNode) whereClause);
                    break;
                case NodeTypes.OR_NODE:
                    generate((OrNode) whereClause);
                    break;
                default:
                    generate(whereClause);
                    break;
            }
        } else {
            jsonBuilder.field("match_all", new HashMap());
        }
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
        tableContext = executionContext.tableContext(name);
        if (tableContext == null) {
            throw new SQLParseException("No table definition found for " + name);
        }
        indices.add(name);
    }

    private void generate(ResultColumnList columnList) throws IOException {
        Set<String> fields = new LinkedHashSet<String>();

        for (ResultColumn column : columnList) {
            if (column instanceof AllResultColumn) {
                for (String name : tableContext.allCols()) {
                    outputFields.add(new Tuple<String, String>(name, name));
                    fields.add(name);
                }
                continue;
            }

            String columnName = column.getExpression().getColumnName();
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
            } else {
                // this should be a normal field which will also be extracted from the source for
                // us in the search hit, so it is always safe to add it to the fields
                fields.add(columnName);
            }
            outputFields.add(new Tuple<String, String>(column.getName(), columnName));
        }
        if (fields.size() > 0) {
            jsonBuilder.field("fields", fields);
        }
    }

    private void generate(Integer operatorType, ColumnReference left, ConstantNode right)
            throws IOException, StandardException {
        // if an operator is added here the swapOperator method should also be extended.

        if (operatorType == SQLOperatorTypes.EQUALS) {
            jsonBuilder.startObject("term")
                    .field(left.getColumnName(), right.getValue())
                    .endObject();
        } else if (rangeQueryOperatorMap.containsKey(operatorType)) {
            jsonBuilder.startObject("range")
                    .startObject(left.getColumnName())
                    .field(rangeQueryOperatorMap.get(operatorType), right.getValue())
                    .endObject()
                    .endObject();
        } else if (operatorType == SQLOperatorTypes.NOT_EQUALS) {
            jsonBuilder.startObject("bool")
                    .startObject("must_not")
                    .startObject("term").field(left.getColumnName(), right.getValue()).endObject()
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
        if (left.getNodeType() == NodeTypes.COLUMN_REFERENCE
                && (right instanceof NumericConstantNode || right instanceof CharConstantNode)) {
            generate(operatorType, (ColumnReference) left, (ConstantNode) right);
            return;
        } else if ((left instanceof NumericConstantNode || left instanceof CharConstantNode)
                && right.getNodeType() == NodeTypes.COLUMN_REFERENCE) {
            generate(swapOperator(operatorType), (ColumnReference) right, (ConstantNode) left);
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

    public XContentBuilder getXContentBuilder() {
        try {
            jsonBuilder = jsonBuilder.endObject();
        } catch (IOException ex) {
        }
        return jsonBuilder;
    }

    /**
     * The indices are only available after @{link #generate(CursorNode)} has been called.
     *
     * @return tables from the sql select statement.
     */
    public List<String> getIndices() {
        return indices;
    }

}

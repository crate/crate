package org.cratedb.action.parser;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class TableVisitor extends XContentVisitor {

    private boolean stopTraversal;
    private Map<String, Object> indexSettings = newHashMap();
    private Map<String, Object> mappingProperties = newHashMap();
    private Map<String, Object> mappingMeta = newHashMap();
    private Map<String, Object> mapping = newHashMap();
    private ColumnReference routingColumn = null;

    private final String[] allowedColumnTypes = {"string", "integer", "long", "short", "double",
                                                 "float", "byte", "boolean", "timestamp"};

    @Override
    public XContentBuilder getXContentBuilder() throws StandardException {
        throw new UnsupportedOperationException("Use settings() or mappings() to get the " +
                "results");
    }

    public TableVisitor(ParsedStatement stmt) throws StandardException {
        super(stmt);
        stopTraversal = false;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        DDLStatementNode ddlNode = (DDLStatementNode) node;
        switch(ddlNode.getNodeType()) {
            case NodeTypes.CREATE_TABLE_NODE:
                stopTraversal = true;
                return visit((CreateTableNode)node);
            default:
                throw new SQLParseException("Unsupported DDL Statement");
        }
    }

    public Visitable visit(CreateTableNode node) throws StandardException {
        // validation
        if (node.isWithData()) {
            throw new SQLParseException("Create Table With Data is not Supported.");
        }
        if (node.getQueryExpression() != null) {
            throw new SQLParseException("Create Table from Query is not Supported.");
        }
        // TODO handle existence checks (IF NOT EXISTS, IF EXISTS ...)
        String tableName = node.getObjectName().getTableName();
        stmt.addIndex(tableName);

        indexSettings.put("number_of_replicas", node.numberOfReplicas(0));
        indexSettings.put("number_of_shards", node.numberOfShards(5));

        routingColumn = node.routingColumn();

        // build mapping
        for (TableElementNode tableElement : node.getTableElementList()) {
            switch(tableElement.getNodeType()) {
                case NodeTypes.COLUMN_DEFINITION_NODE:
                    visit((ColumnDefinitionNode) tableElement);
                    break;
                case NodeTypes.CONSTRAINT_DEFINITION_NODE:
                    visit((ConstraintDefinitionNode)tableElement);
            }
        }
        stopTraversal = true;

        return node;
    }

    public Visitable visit(ColumnDefinitionNode node) throws SQLParseException {

        Map<String, String> columnDefinition = newHashMap();

        String columnType = node.getType().getTypeName().toLowerCase();
        List<String> allowedColumnTypeList = Arrays.asList(allowedColumnTypes);
        if (!allowedColumnTypeList.contains(columnType)) {
            throw new SQLParseException("Unsupported type");
        }

        columnDefinition.put("type", columnType);
        // TODO: use parsed values (not yet supported by parser)
        columnDefinition.put("index", "not_analyzed");
        columnDefinition.put("store", "true");

        mappingProperties.put(node.getColumnName(), columnDefinition);
        return node;
    }


    public Visitable visit(ConstraintDefinitionNode node) {
        switch(node.getConstraintType()) {
            case PRIMARY_KEY:
                String[] pkColumnNames = node.getColumnList().getColumnNames();
                if (pkColumnNames.length > 1) {
                    throw new SQLParseException("Multiple Primary Key Columns not Supported.");
                }
                mappingMeta.put("primary_keys", pkColumnNames[0]);
                break;
            // TODO: handle INDEX-DEFINITION
            default:
                throw new SQLParseException("Unsupported Constraint");
        }
        return node;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraversal;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

    public Map<String, Object> mapping() {
        if (mapping != null) {
            if (mappingMeta != null) {
                mapping.put("_meta", mappingMeta);
            }
            if (routingColumn != null) {
                Map<String, String> _routing = newHashMap();
                _routing.put("path", routingColumn.getColumnName());
                mapping.put("_routing", _routing);
            }
            mapping.put("properties", mappingProperties);
        }
        return mapping;
    }

    public Map<String, Object> settings() {
        return indexSettings;
    }
}

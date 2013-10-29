package org.cratedb.action.parser.visitors;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class TableVisitor extends BaseVisitor {

    private final String[] allowedColumnTypes = {"string", "integer", "long", "short", "double",
                                                 "float", "byte", "boolean", "timestamp"};
    private ColumnReference routingColumn;
    private List<String> primaryKeyColumns;
    private Map<String, Object> mappingProperties = newHashMap();
    private Map<String, Object> mappingMeta = newHashMap();
    private Map<String, Object> mapping = newHashMap();
    private Map<String, Object> indexSettings = newHashMap();

    public TableVisitor(ParsedStatement stmt) throws StandardException {
        super(null, stmt, new Object[0]);
    }

    @Override
    protected void afterVisit() throws StandardException {
        super.afterVisit();

        stmt.indexSettings = ImmutableMap.copyOf(this.indexSettings);
        stmt.indexMapping = ImmutableMap.copyOf(this.mapping());
    }

    @Override
    public void visit(CreateTableNode node) throws Exception {
        // validation
        if (node.isWithData()) {
            throw new SQLParseException("Create Table With Data is not Supported.");
        }
        if (node.getQueryExpression() != null) {
            throw new SQLParseException("Create Table from Query is not Supported.");
        }
        // TODO handle existence checks (IF NOT EXISTS, IF EXISTS ...)
        String tableName = node.getObjectName().getTableName();
        stmt.tableName(tableName);

        indexSettings.put("number_of_replicas", node.numberOfReplicas(1));
        indexSettings.put("number_of_shards", node.numberOfShards(5));

        // build mapping
        for (TableElementNode tableElement : node.getTableElementList()) {
            visit(tableElement);
        }

        routingColumn = node.routingColumn();
        // currently it's not supported to set the routing column different to the
        // primary key column
        if (routingColumn != null && !primaryKeyColumns.contains(routingColumn.getColumnName())) {
            throw new SQLParseException("Only columns declared as primary key can be used for " +
                    "routing");
        }

        stmt.type(ParsedStatement.ActionType.CREATE_INDEX_ACTION);
    }

    @Override
    public void visit(DropTableNode node) throws StandardException {
        stmt.tableName(node.getObjectName().getTableName());
        stmt.type(ParsedStatement.ActionType.DELETE_INDEX_ACTION);
    }

    @Override
    public void visit(ColumnDefinitionNode node) throws SQLParseException {

        Map<String, String> columnDefinition = newHashMap();

        String columnType = node.getType().getTypeName().toLowerCase();
        List<String> allowedColumnTypeList = Arrays.asList(allowedColumnTypes);
        if (!allowedColumnTypeList.contains(columnType)) {
            throw new SQLParseException("Unsupported type");
        }

        columnDefinition.put("type", columnType);
        // TODO: use parsed values (not yet supported by parser)
        columnDefinition.put("index", "not_analyzed");
        columnDefinition.put("store", "false");

        mappingProperties.put(node.getColumnName(), columnDefinition);
    }


    @Override
    public void visit(ConstraintDefinitionNode node) {
        switch(node.getConstraintType()) {
            case PRIMARY_KEY:
                primaryKeyColumns = Arrays.asList(node.getColumnList().getColumnNames());
                if (primaryKeyColumns.size() > 1) {
                    throw new SQLParseException("Multiple Primary Key Columns not Supported.");
                }
                mappingMeta.put("primary_keys", primaryKeyColumns.get(0));
                break;
            // TODO: handle INDEX-DEFINITION
            default:
                throw new SQLParseException("Unsupported Constraint");
        }
    }

    private Map<String, Object> mapping() {
        if (mapping != null) {
            if (mappingMeta != null) {
                mapping.put("_meta", mappingMeta);
            }
            if (routingColumn != null) {
                Map<String, String> routing = newHashMap();
                routing.put("path", routingColumn.getColumnName());
                mapping.put("_routing", routing);
            }
            mapping.put("properties", mappingProperties);
        }
        return mapping;
    }
}

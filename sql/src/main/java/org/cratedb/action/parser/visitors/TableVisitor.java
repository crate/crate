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
    protected void afterVisit() throws SQLParseException {
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

        Map<String, Object> columnDefinition = safeColumnDefinition(node.getColumnName());

        String columnType = node.getType().getTypeName().toLowerCase();
        List<String> allowedColumnTypeList = Arrays.asList(allowedColumnTypes);
        if (!allowedColumnTypeList.contains(columnType)) {
            throw new SQLParseException("Unsupported type");
        }

        // support index definition before column definition
        if (columnDefinition.containsKey("fields")) {
            Map<String, Map<String, String>> columnFieldsDefinition = (Map)columnDefinition.get("fields");
            columnDefinition = (Map)columnFieldsDefinition.get(node.getColumnName());
            assert columnDefinition != null;

            // fix types of the indexes (they are null because column was not defined already)
            for (Map.Entry<String, Map<String,String>> entry : columnFieldsDefinition.entrySet()) {
                if (entry.getKey().equals(node.getColumnName())) {
                    continue;
                }
                entry.getValue().put("type", columnType);
            }
        }

        columnDefinition.put("type", columnType);

        // default values
        if (!columnDefinition.containsKey("index")) {
            columnDefinition.put("index", "not_analyzed");
        }
        if (!columnDefinition.containsKey("store")) {
            columnDefinition.put("store", "false");
        }
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
            default:
                throw new SQLParseException("Unsupported Constraint");
        }
    }

    @Override
    public void visit(IndexConstraintDefinitionNode node) throws StandardException {
        String indexName = node.getIndexName();
        if (node.getIndexColumnList() != null) {
            for (IndexColumn indexColumn : node.getIndexColumnList()) {
                String columnName = indexColumn.getColumnName();
                Map<String, Object> columnDefinition = safeColumnDefinition(columnName);
                Map<String, Object> indexColumnDefinition = newHashMap();
                indexColumnDefinition.putAll(columnDefinition);

                if (!indexName.equals(columnName)) {
                    // prepare for multi_field mapping
                    indexColumnDefinition.clear();
                    indexColumnDefinition.put("type", columnDefinition.get("type"));
                    indexColumnDefinition.put("store", "false");
                }

                switch (node.getIndexMethod()) {
                    case "fulltext":
                        indexColumnDefinition.put("index", "analyzed");
                        GenericProperties indexProperties = node.getIndexProperties();
                        if (indexProperties != null && indexProperties.get("analyzer") != null) {
                            QueryTreeNode analyzer = indexProperties.get("analyzer");
                            if (!(analyzer instanceof ValueNode)) {
                                throw new SQLParseException("'analyzer' property invalid");
                            }
                            indexColumnDefinition.put("analyzer",
                                    (String)valueFromNode((ValueNode)analyzer));
                        } else {
                            indexColumnDefinition.put("analyzer", "standard");
                        }
                        break;
                    default:
                        throw new SQLParseException("Unsupported index method '" +
                                node.getIndexMethod() + "'");
                }

                if (!indexName.equals(columnName)) {
                    // create multi_field mapping
                    Map<String, Object> fieldsDefinition = newHashMap();
                    Map<String, Object> originalColumnDefinition = newHashMap();
                    originalColumnDefinition.putAll(columnDefinition);
                    fieldsDefinition.put(columnName, originalColumnDefinition);
                    fieldsDefinition.put(indexName, indexColumnDefinition);
                    columnDefinition.clear();
                    columnDefinition.put("type", "multi_field");
                    columnDefinition.put("path", "just_name");
                    columnDefinition.put("fields", fieldsDefinition);
                } else {
                    columnDefinition.putAll(indexColumnDefinition);
                }
            }
        } else if (!node.isIndexOff()) {
            throw new SQLParseException("Unsupported index constraint");
        }
    }

    private Map<String, Object> safeColumnDefinition(String columnName) {
        Map<String, Object> columnDefinition = (Map)mappingProperties.get(columnName);
        if (columnDefinition == null) {
            columnDefinition = newHashMap();
            mappingProperties.put(columnName, columnDefinition);
        }

        return columnDefinition;
    }

    private Map<String, Object> mapping() {
        if (mapping != null) {
            if (mappingMeta != null && mappingMeta.size() > 0) {
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

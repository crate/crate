/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package org.cratedb.action.parser.visitors;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

import static com.google.common.collect.Maps.newHashMap;

public class TableVisitor extends BaseVisitor {

    private final ImmutableSet<String> allowedColumnTypes = ImmutableSet.of("string", "integer",
                                                                        "long", "short", "double",
                                                                        "float", "byte", "boolean",
                                                                        "timestamp", "object", "ip");
    private ColumnReference routingColumn;
    private List<String> primaryKeyColumns;
    private Map<String, Object> mappingProperties = newHashMap();
    private Map<String, Object> mappingMeta = newHashMap();
    private Map<String, Object> mapping = newHashMap();
    private Map<String, Object> indexSettings = newHashMap();

    public TableVisitor(NodeExecutionContext nodeExecutionContext,
                        ParsedStatement stmt,
                        Object[] args) throws StandardException {
        super(nodeExecutionContext, stmt, args);
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
        // visit columnDefinitions and primary key constraint first, ...
        List<IndexConstraintDefinitionNode> indexConstraints = new ArrayList<>();
        for (TableElementNode tableElement : node.getTableElementList()) {
            if (tableElement.getNodeType() == NodeType.INDEX_CONSTRAINT_NODE) {
                indexConstraints.add((IndexConstraintDefinitionNode)tableElement);
                continue;
            }
            visit(tableElement);
        }
        // ... then handle index constraints
        for (IndexConstraintDefinitionNode indexConstraint : indexConstraints) {
            visit(indexConstraint);
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
        tableName(node.getObjectName());
        if (tableContext.tableIsAlias()) {
            throw new SQLParseException("Table alias not allowed in DROP TABLE statement.");
        }
        stmt.type(ParsedStatement.ActionType.DELETE_INDEX_ACTION);
    }

    public void visit(ColumnDefinitionNode node) throws SQLParseException {
        visit(node, mappingProperties);
    }

    public void visit(ColumnDefinitionNode node, Map<String, Object> mapping) {

        String columnName = node.getColumnName();
        Map<String, Object> columnDefinition;
        columnDefinition = safeColumnDefinition(columnName, mapping);

        String columnType = node.getType().getTypeName().toLowerCase();
        if (!allowedColumnTypes.contains(columnType)) {
            throw new SQLParseException(String.format("Unsupported type: '%s'", columnType));
        }
        // map timestamp to date
        columnType = columnType.equals("timestamp") ? "date" : columnType;

        columnDefinition.put("type", columnType);

        // default values
        if (!columnDefinition.containsKey("index")) {
            columnDefinition.put("index", "not_analyzed");
        }
        if (!columnDefinition.containsKey("store")) {
            columnDefinition.put("store", "false");
        }
    }

    public void visit(ObjectColumnDefinitionNode node) throws SQLParseException {
        visit(node, mappingProperties);
    }

    public void visit(ObjectColumnDefinitionNode node, Map<String, Object> mapping) throws SQLParseException {

        String columnName = node.getColumnName();
        Map<String, Object> columnDefinition = safeColumnDefinition(columnName, mapping);

        String columnType = node.getType().getTypeName().toLowerCase();
        if (!allowedColumnTypes.contains(columnType)) {
            throw new SQLParseException(String.format("Unsupported type: '%s'", columnType));
        }
        columnDefinition.put("type", columnType);

        switch (node.objectType()) {
            case DYNAMIC:
                columnDefinition.put("dynamic", "true");
                break;
            case STRICT:
                columnDefinition.put("dynamic", "strict");
                break;
            case IGNORED:
                columnDefinition.put("dynamic", "false");
                break;
        }
        if (node.subColumns() != null && node.subColumns().size() > 0) {
            Map<String, Object> subColumnsMapping = new HashMap<>();
            columnDefinition.put("properties", subColumnsMapping);

            for (TableElementNode subColumn: node.subColumns()) {
                switch(subColumn.getNodeType()) {
                    case COLUMN_DEFINITION_NODE:
                        visit((ColumnDefinitionNode) subColumn, subColumnsMapping);
                        break;
                    case OBJECT_COLUMN_DEFINITION_NODE:
                        visit((ObjectColumnDefinitionNode)subColumn, subColumnsMapping);
                        break;
                    default:

                }
            }
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

    /**
     *
     * @param node
     * @throws StandardException
     */
    @Override
    public void visit(IndexConstraintDefinitionNode node) throws StandardException {
        String indexName = node.getIndexName();

        if (node.isIndexOff()) {
            Map<String, Object> columnDefinition = (Map<String, Object>)mappingProperties.get(indexName);
            if (columnDefinition == null) {
                throw new SQLParseException(String.format("Unknown Column '%s'", indexName));
            }
            columnDefinition.put("index", "no");
        } else if (node.getIndexColumnList() != null) {
            for (IndexColumn indexColumn : node.getIndexColumnList()) {
                String columnName = indexColumn.getColumnName();

                // assume column definition exists
                Map<String, Object> columnDefinition;
                if (columnName.contains(".")) {
                   columnDefinition = popColumnDefinition(columnName, mappingProperties);
                   mappingProperties.put(columnName, columnDefinition);
                } else {
                    columnDefinition = (Map<String, Object>)mappingProperties.get(columnName);
                    if (columnDefinition == null) {
                        throw new SQLParseException(String.format("Unknown Column '%s'", columnName));
                    }
                }

                Map<String, Object> indexColumnDefinition = newHashMap();
                indexColumnDefinition.putAll(columnDefinition);

                if (!indexName.equals(columnName)) {
                    // prepare for multi_field mapping
                    indexColumnDefinition.clear();
                    String type = (String)columnDefinition.get("type");
                    if (type != null && type.equals("object")) {
                        throw new SQLParseException("Cannot index object columns");
                    }
                    indexColumnDefinition.put("type", type);
                    indexColumnDefinition.put("store", "false");
                }

                if (node.getIndexMethod().equalsIgnoreCase("fulltext")) {
                    indexColumnDefinition.put("index", "analyzed");
                    GenericProperties indexProperties = node.getIndexProperties();
                    if (indexProperties != null && indexProperties.get("analyzer") != null) {
                        QueryTreeNode analyzer = indexProperties.get("analyzer");
                        if (!(analyzer instanceof ValueNode)) {
                            throw new SQLParseException("'analyzer' property invalid");
                        }
                        // resolve custom analyzer and put into settings
                        String analyzerName = (String)valueFromNode((ValueNode)analyzer);
                        if (context.analyzerService().hasCustomAnalyzer(analyzerName)) {
                            Settings customAnalyzerSettings = context.analyzerService().resolveFullCustomAnalyzerSettings(analyzerName);
                            indexSettings.putAll(customAnalyzerSettings.getAsMap());
                        } else if (!context.analyzerService().hasBuiltInAnalyzer(analyzerName)) {
                            throw new SQLParseException("Analyzer does not exist");
                        }
                        indexColumnDefinition.put("analyzer", analyzerName);
                    } else {
                        indexColumnDefinition.put("analyzer", "standard");
                    }
                } else if (node.getIndexMethod().equalsIgnoreCase("plain")) {
                    // default behaviour
                    indexColumnDefinition.put("index", "not_analyzed");
                } else {
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
        } else {
            throw new SQLParseException("Unsupported index constraint");
        }
    }

    private Map<String, Object> safeColumnDefinition(String columnName, Map<String, Object> mapping) {
        Map<String, Object> map = mapping;
        Map<String, Object> subMap;
        for (String pathElement : Splitter.on('.').split(columnName)) {

            if (map.containsKey("properties")) {
                subMap = (Map<String, Object>)((Map<String, Object>)map.get("properties")).get(pathElement);
            } else {
                subMap = (Map<String, Object>)map.get(pathElement);
            }

            if (subMap == null) {
                subMap = new HashMap<>();
                if (map.containsKey("properties") || map != mapping) {
                    Map<String, Object> props = (Map<String, Object>)map.get("properties");
                    if (props == null) {
                        props = new HashMap<>();
                        map.put("properties", props);
                    }
                    props.put(pathElement, subMap);

                } else {
                    map.put(pathElement, subMap);
                }
            }
            map = subMap;
        }

        return map;
    }

    private Map<String, Object> popColumnDefinition(String columnName, Map<String, Object> mapping) throws SQLParseException {
        String lastElement = null;
        Map<String, Object> columnDefinition = mapping;
        Map<String, Object> map = null;
        for (String pathElement : Splitter.on('.').split(columnName)) {
            map = columnDefinition;
            lastElement = pathElement;
            if (map.containsKey("properties")) {
                map = (Map<String, Object>)map.get("properties");
            }
            columnDefinition = (Map<String, Object>)map.get(pathElement);

            if (columnDefinition == null) {
                throw new SQLParseException(String.format("Unknown Column '%s'", columnName));
            }
        }
        if (lastElement != null && map != null) {
            map.remove(lastElement);
        }
        return columnDefinition;

    }

    private Map<String, Object> mapping() {
        if (mapping != null) {
            if (mappingMeta != null && mappingMeta.size() > 0) {
                mapping.put("_meta", mappingMeta);
            }
            mapping.put("properties", mappingProperties);
        }
        return mapping;
    }
}

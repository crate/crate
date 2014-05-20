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
package io.crate.analyze;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.*;


public class CreateTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateTableAnalysis> {

    protected Void visitNode(Node node, CreateTableAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Void visitCreateTable(CreateTable node, CreateTableAnalysis context) {
        TableIdent tableIdent = TableIdent.of(node.name());
        Preconditions.checkArgument(Strings.isNullOrEmpty(tableIdent.schema()),
                "A custom schema name must not be specified in the CREATE TABLE clause");
        context.table(tableIdent);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        if (node.properties().isPresent()) {
            Settings settings =
                    TablePropertiesAnalysis.propertiesToSettings(node.properties().get(), context.parameters(), true);
            context.indexSettingsBuilder().put(settings);
        }

        for (TableElement tableElement : node.tableElements()) {
            process(tableElement, context);
        }

        validatePrimaryKeys(context);

        for (CrateTableOption option : node.crateTableOptions()) {
            process(option, context);
        }

        validateAnalyzer(context);
        setCopyTo(context);

        return null;
    }

    private void validateAnalyzer(CreateTableAnalysis context) {
        for (Map.Entry<String, Object> entry : context.mappingProperties().entrySet()) {
            assert entry.getValue() instanceof Map;
            Map properties = (Map)entry.getValue();
            Object analyzer = properties.get("analyzer");
            if (analyzer != null && !analyzer.equals("not_analyzed") && !properties.get("type").equals("string")) {
                throw new IllegalArgumentException(
                        String.format("Can't use an Analyzer on column \"%s\" because analyzer are only allowed on columns with type \"string\".",
                                entry.getKey()
                        ));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void setCopyTo(CreateTableAnalysis context) {
        for (Map.Entry<String, Set<String>> entry : context.copyTo().entrySet()) {

            Map<String, Object> columnDef = context.mappingProperties();
            String[] key = entry.getKey().split("\\.");

            try {
                while (key.length > 1) {
                    columnDef = (Map<String, Object>)((Map<String, Object>)columnDef.get(key[0])).get("properties");
                    key = Arrays.copyOfRange(key, 1, key.length);
                }
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("fulltext index points to a column that doesn't exist");
            }

            columnDef = (Map<String, Object>)columnDef.get(key[0]);
            if (columnDef == null) {
                throw new ColumnUnknownException(entry.getKey());
            }
            columnDef.put("copy_to", Lists.newArrayList(entry.getValue()));
        }
    }

    @Override
    public Void visitColumnDefinition(ColumnDefinition node, CreateTableAnalysis context) {
        if (node.ident().startsWith("_")) {
            throw new IllegalArgumentException("Column ident must not start with '_'");
        }

        CreateTableAnalysis.ColumnSchema schema = context.pushColumn(node.ident());
        schema.esMapping.put("store", false);

        for (ColumnConstraint columnConstraint : node.constraints()) {
            process(columnConstraint, context);
        }
        if (!schema.esMapping.containsKey("index")) {
            // not set by the column constraints -> use default
            schema.esMapping.put("index", "not_analyzed");
        }

        process(node.type(), context);

        context.pop();

        return null;
    }

    @Override
    public Void visitIndexDefinition(IndexDefinition node, CreateTableAnalysis context) {
        CreateTableAnalysis.ColumnSchema columnSchema = context.pushIndex(node.ident());

        columnSchema.esMapping.put("store", false);
        columnSchema.esMapping.put("type", "string");
        setAnalyzer(columnSchema.esMapping, node.properties(), context);

        for (Expression expression : node.columns()) {
            String expressionName = ExpressionToStringVisitor.convert(expression, context.parameters());
            context.addCopyTo(expressionName, node.ident());
        }
        context.pop();

        return null;
    }

    @Override
    public Void visitColumnType(ColumnType node, CreateTableAnalysis context) {
        String typeName;
        if (node.name().equals("timestamp")) {
            typeName = "date";
        } else if (node.name().equals("int")) {
            typeName = "integer";
        } else {
            typeName = node.name();
        }

        Object indexName = context.currentColumnDefinition().get("index");
        assert indexName != null;
        if (indexName.equals("not_analyzed")) {
            context.currentColumnDefinition().put("doc_values", true);
        }
        context.currentColumnDefinition().put("type", typeName);

        return null;
    }

    @Override
    public Void visitObjectColumnType(ObjectColumnType node, CreateTableAnalysis context) {
        context.currentColumnDefinition().put("type", node.name());

        switch (node.objectType().or("dynamic").toLowerCase(Locale.ENGLISH)) {
            case "dynamic":
                context.currentColumnDefinition().put("dynamic", "true");
                break;
            case "strict":
                context.currentColumnDefinition().put("dynamic", "strict");
                break;
            case "ignored":
                context.currentColumnDefinition().put("dynamic", "false");
                break;
        }

        context.pushNestedProperties();
        for (ColumnDefinition columnDefinition : node.nestedColumns()) {
            process(columnDefinition, context);
        }
        context.pop();

        return null;
    }

    @Override
    public Void visitCollectionColumnType(CollectionColumnType node, CreateTableAnalysis context) {
        if (node.type() == ColumnType.Type.SET) {
            throw new UnsupportedOperationException("the SET dataType is currently not supported");
        }
        context.currentMetaColumnDefinition().put("collection_type", "array");
        context.currentColumnDefinition().put("doc_values", false);

        if (node.innerType().type() != ColumnType.Type.PRIMITIVE) {
            throw new UnsupportedOperationException("Nesting ARRAY or SET types is currently not supported");
        }

        process(node.innerType(), context);
        return null;
    }

    @Override
    public Void visitIndexColumnConstraint(IndexColumnConstraint node, CreateTableAnalysis context) {
        if (node.indexMethod().equalsIgnoreCase("fulltext")) {
            setAnalyzer(context.currentColumnDefinition(), node.properties(), context);
        } else if (node.indexMethod().equalsIgnoreCase("plain")) {
            context.currentColumnDefinition().put("index", "not_analyzed");
        } else if (node.indexMethod().equalsIgnoreCase("OFF")) {
            context.currentColumnDefinition().put("index", "no");
        } else {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid index method \"%s\"", node.indexMethod()));
        }
        return null;
    }

    @Override
    public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint node, CreateTableAnalysis context) {
        for (Expression expression : node.columns()) {
            context.addPrimaryKey(ExpressionToStringVisitor.convert(expression, context.parameters()));
        }
        return null;
    }

    @Override
    public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, CreateTableAnalysis context) {
        context.addPrimaryKey(context.currentColumnName());
        return null;
    }

    public void validatePrimaryKeys(CreateTableAnalysis context) {
        for (String pKey : context.primaryKeys()) {
            if (!context.hasColumnDefinition(pKey)) {
                throw new ColumnUnknownException(pKey);
            }
        }
    }

    private void setAnalyzer(Map<String, Object> columnDefinition,
                             GenericProperties properties,
                             CreateTableAnalysis context) {
        columnDefinition.put("index", "analyzed");
        Expression analyzerExpression = properties.get("analyzer");
        if (analyzerExpression == null) {
            columnDefinition.put("analyzer", "standard");
            return;
        }

        if (analyzerExpression instanceof ArrayLiteral) {
            throw new IllegalArgumentException("array literal not allowed for the analyzer property");
        }

        String analyzerName = ExpressionToStringVisitor.convert(analyzerExpression, context.parameters());
        if (context.analyzerService().hasCustomAnalyzer(analyzerName)) {
            Settings settings = context.analyzerService().resolveFullCustomAnalyzerSettings(analyzerName);
            context.indexSettingsBuilder().put(settings.getAsMap());
        }

        columnDefinition.put("analyzer", analyzerName);
    }

    @Override
    public Void visitClusteredBy(ClusteredBy node, CreateTableAnalysis context) {
        if (node.column().isPresent()) {
            String routingColumn = ExpressionToStringVisitor.convert(
                    node.column().get(), context.parameters());

            if (!context.hasColumnDefinition(routingColumn)) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid or non-existent routing column \"%s\"",
                                routingColumn));
            }
            if (Collections2.transform(context.partitionedBy(), new Function<List<String>, String>() {
                @Nullable
                @Override
                public String apply(@Nullable List<String> input) {
                    if (input != null && input.size() > 0) {
                        return input.get(0);
                    }
                    return null;
                }
            }).contains(routingColumn)) {
                throw new IllegalArgumentException("Cannot use CLUSTERED BY column in PARTITIONED BY clause");
            }
            if (context.primaryKeys().size() > 0 && !context.primaryKeys().contains(routingColumn)) {
                throw new IllegalArgumentException("Clustered by column must be part of primary keys");
            }

            context.routing(routingColumn);
        }
        int numShards = node.numberOfShards().or(Constants.DEFAULT_NUM_SHARDS);
        if (numShards < 1) {
            throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
        }
        context.indexSettingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards);
        return null;
    }

    @Override
    public Void visitPartitionedBy(PartitionedBy node, CreateTableAnalysis context) {
        for (Expression partitionByColumn : node.columns()) {
            String columnName = ExpressionToStringVisitor.convert(partitionByColumn, context.parameters());

            Map<String, Object> columnDefinition = context.popColumnDefinition(columnName);
            String type;
            if (columnDefinition != null) {
                if (context.primaryKeys().size() > 0 && !context.primaryKeys().contains(columnName)) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "Cannot use non primary key column '%s' in PARTITIONED BY clause if primary key is set on table",
                                    columnName));
                }
                String routing = context.routing();
                if (routing != null && routing.equals(columnName)) {
                    throw new IllegalArgumentException(
                                    "Cannot use CLUSTERED BY column in PARTITIONED BY clause"
                    );
                }
                if (context.isInArray(columnName)) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "Cannot use array column '%s' in PARTITIONED BY clause", columnName));
                }

                if ((columnDefinition.get("type") != null && columnDefinition.get("type").equals("object"))) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "Cannot use object column '%s' in PARTITIONED BY clause", columnName));
                }
                if (columnDefinition.get("index") != null && columnDefinition.get("index").equals("analyzed")) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH,
                                    "Cannot use column '%s' with fulltext index in PARTITIONED BY clause", columnName));
                }
                type = (String)columnDefinition.get("type");
            } else if(columnName.startsWith("_")) {
                throw new IllegalArgumentException("Cannot use system columns in PARTITIONED BY clause");
            } else {
                throw new ColumnUnknownException(columnName);
            }
            context.addPartitionedByColumn(columnName, type);
        }
        return null;
    }
}

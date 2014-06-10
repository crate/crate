/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;
import org.elasticsearch.common.settings.Settings;

import java.util.Locale;
import java.util.Map;

public class AlterTableAddColumnAnalyzer extends AbstractStatementAnalyzer<Void, AddColumnAnalysis> {

    @Override
    protected Void visitNode(Node node, AddColumnAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a AlterTableAddColumn node", node));
    }

    @Override
    public Void visitAlterTableAddColumnStatement(AlterTableAddColumn node, AddColumnAnalysis context) {
        setTableAndPartitionName(node.table(), context);
        process(node.tableElement(), context);
        return null;
    }

    @Override
    public Void visitColumnDefinition(ColumnDefinition node, AddColumnAnalysis context) {
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
    public Void visitIndexDefinition(IndexDefinition node, AddColumnAnalysis context) {
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
    public Void visitColumnType(ColumnType node, AddColumnAnalysis context) {
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
    public Void visitObjectColumnType(ObjectColumnType node, AddColumnAnalysis context) {
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
    public Void visitCollectionColumnType(CollectionColumnType node, AddColumnAnalysis context) {
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
    public Void visitIndexColumnConstraint(IndexColumnConstraint node, AddColumnAnalysis context) {
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

    private void setTableAndPartitionName(Table node, AddColumnAnalysis context) {
        context.table(TableIdent.of(node));
        if (!node.partitionProperties().isEmpty()) {
            PartitionName partitionName = PartitionPropertiesAnalyzer.toPartitionName(
                    context.table(),
                    node.partitionProperties(),
                    context.parameters());
            if (!context.table().partitions().contains(partitionName)) {
                throw new IllegalArgumentException("Referenced partition does not exist.");
            }
            context.partitionName(partitionName);
        }
    }
}

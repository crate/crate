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

import com.google.common.collect.Lists;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.*;
import org.cratedb.Constants;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.parser.StandardException;
import org.elasticsearch.common.settings.Settings;

import java.util.*;

public class CreateTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateTableAnalysis> {

    private final ExpressionVisitor expressionVisitor = new ExpressionVisitor();

    @Override
    protected Void visitNode(Node node, CreateTableAnalysis context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Void visitCreateTable(CreateTable node, CreateTableAnalysis context) {

        context.table(TableIdent.of(node.name()));
        context.indexSettingsBuilder().put("number_of_replicas",
                node.replicas().or(Constants.DEFAULT_NUM_REPLICAS));

        if (node.clusteredBy().isPresent()) {
            ClusteredBy clusteredBy = node.clusteredBy().get();
            context.indexSettingsBuilder().put("number_of_shards",
                    clusteredBy.numberOfShards().or(Constants.DEFAULT_NUM_SHARDS));
        }

        for (TableElement tableElement : node.tableElements()) {
            process(tableElement, context);
        }

        setCopyTo(context);

        return null;
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
            columnDef.put("copy_to", Lists.newArrayList(entry.getValue()));
        }
    }

    @Override
    public Void visitColumnDefinition(ColumnDefinition node, CreateTableAnalysis context) {
        Map<String, Object> columnDefinition = new HashMap<>();
        context.addColumnDefinition(node.ident(), columnDefinition);
        columnDefinition.put("store", false);

        if (node.constraints().isEmpty()) { // set default if no constraints
            columnDefinition.put("index", "not_analyzed");
        }
        for (ColumnConstraint columnConstraint : node.constraints()) {
            process(columnConstraint, context);
        }

        process(node.type(), context);
        context.propertiesStack().pop();

        return null;
    }

    @Override
    public Void visitIndexDefinition(IndexDefinition node, CreateTableAnalysis context) {
        Map<String, Object> columnDefinition = new HashMap<>();
        context.addIndexDefinition(node.ident(), columnDefinition);

        columnDefinition.put("store", false);
        columnDefinition.put("type", "string");
        setAnalyzer(columnDefinition, node.properties(), context);

        for (Expression expression : node.columns()) {
            String expressionName = expressionVisitor.process(expression, null);
            context.addCopyTo(expressionName, node.ident());
        }

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
        context.currentColumnDefinition().put("type", typeName);

        return null;
    }

    @Override
    public Void visitObjectColumnType(ObjectColumnType node, CreateTableAnalysis context) {
        context.currentColumnDefinition().put("type", node.name());
        Map<String, Object> nestedProperties = new HashMap<>();
        context.currentColumnDefinition().put("properties", nestedProperties);

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

        context.propertiesStack().push(nestedProperties);
        for (ColumnDefinition columnDefinition : node.nestedColumns()) {
            process(columnDefinition, context);
        }
        context.propertiesStack().pop();

        return null;
    }

    @Override
    public Void visitCollectionColumnType(CollectionColumnType node, CreateTableAnalysis context) {
        throw new UnsupportedOperationException("the ARRAY and SET dataTypes are currently not supported");
        //if (node.type() == ColumnType.Type.SET) {
        // throw new UnsupportedOperationException("the SET dataType is currently not supported");
        //}

        //context.currentMetaColumnDefinition().put("collection_type", "array");
        //context.currentColumnDefinition().put("doc_values", false);

        //if (node.innerType().type() != ColumnType.Type.PRIMITIVE) {
        //    throw new UnsupportedOperationException("Nesting ARRAY or SET types is currently not supported");
        //}
        //context.currentColumnDefinition().put("type", node.innerType().name());

        //return null;
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
                    String.format("Invalid index method \"%s\"", node.indexMethod()));
        }
        return null;
    }

    @Override
    public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint node, CreateTableAnalysis context) {
        for (Expression expression : node.columns()) {
            context.addPrimaryKey(expressionVisitor.process(expression, null));
        }
        return null;
    }

    @Override
    public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, CreateTableAnalysis context) {
        context.addPrimaryKey(context.currentColumnName());
        return null;
    }

    private void setAnalyzer(Map<String, Object> columnDefinition,
                             GenericProperties properties,
                             CreateTableAnalysis context) {
        columnDefinition.put("index", "analyzed");
        List<Expression> analyzerExpressions = properties.get("analyzer");
        if (analyzerExpressions == null) {
            columnDefinition.put("analyzer", "standard");
            return;
        }

        if (analyzerExpressions.size() != 1) {
            throw new IllegalArgumentException("Invalid argument(s) passed to the analyzer property");
        }

        String analyzerName = expressionVisitor.process(analyzerExpressions.get(0), context.parameters());
        if (context.analyzerService().hasCustomAnalyzer(analyzerName)) {
            try {
                Settings settings = context.analyzerService().resolveFullCustomAnalyzerSettings(analyzerName);
                context.indexSettingsBuilder().put(settings.getAsMap());
            } catch (StandardException e) {
                throw new CrateException(e);
            }
        }

        columnDefinition.put("analyzer", analyzerName);
    }

    private class ExpressionVisitor extends AstVisitor<String, Object[]> {

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Object[] parameters) {
            return node.getName().getSuffix();
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Object[] parameters) {
            return node.getValue();
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, Object[] parameters) {
            return parameters[node.index()].toString();
        }

        @Override
        protected String visitSubscriptExpression(SubscriptExpression node, Object[] context) {
            return String.format("%s.%s", process(node.name(), null), process(node.index(), null));
        }

        @Override
        protected String visitNode(Node node, Object[] context) {
            throw new UnsupportedOperationException(String.format("Can't handle %s.", node));
        }
    }
}

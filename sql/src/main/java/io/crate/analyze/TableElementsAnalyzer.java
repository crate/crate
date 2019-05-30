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

import io.crate.analyze.expressions.ExpressionToColumnIdentVisitor;
import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.data.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.TableElement;
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.singletonList;

public class TableElementsAnalyzer {

    private static final InnerTableElementsAnalyzer ANALYZER = new InnerTableElementsAnalyzer();
    private static final String COLUMN_STORE_PROPERTY = "columnstore";

    public static AnalyzedTableElements analyze(List<TableElement> tableElements,
                                                Row parameters,
                                                FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                                RelationName relationName,
                                                @Nullable TableInfo tableInfo) {
        AnalyzedTableElements analyzedTableElements = new AnalyzedTableElements();
        int positionOffset = tableInfo == null ? 0 : tableInfo.columns().size();
        for (int i = 0; i < tableElements.size(); i++) {
            TableElement tableElement = tableElements.get(i);
            int position = positionOffset + i + 1;
            ColumnDefinitionContext ctx = new ColumnDefinitionContext(
                position, null, parameters, fulltextAnalyzerResolver, analyzedTableElements, relationName, tableInfo);
            ANALYZER.process(tableElement, ctx);
            if (ctx.analyzedColumnDefinition.ident() != null) {
                analyzedTableElements.add(ctx.analyzedColumnDefinition);
            }
        }
        return analyzedTableElements;
    }

    public static AnalyzedTableElements analyze(TableElement tableElement,
                                                Row parameters,
                                                FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                                TableInfo tableInfo) {
        return analyze(singletonList(tableElement), parameters, fulltextAnalyzerResolver, tableInfo.ident(), tableInfo);
    }

    private static class ColumnDefinitionContext {

        private final Row parameters;
        final FulltextAnalyzerResolver fulltextAnalyzerResolver;
        AnalyzedColumnDefinition analyzedColumnDefinition;
        final AnalyzedTableElements analyzedTableElements;
        final RelationName relationName;
        @Nullable
        final TableInfo tableInfo;

        ColumnDefinitionContext(Integer position,
                                @Nullable AnalyzedColumnDefinition parent,
                                Row parameters,
                                FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                AnalyzedTableElements analyzedTableElements,
                                RelationName relationName,
                                @Nullable TableInfo tableInfo) {
            this.analyzedColumnDefinition = new AnalyzedColumnDefinition(position, parent);
            this.parameters = parameters;
            this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
            this.analyzedTableElements = analyzedTableElements;
            this.relationName = relationName;
            this.tableInfo = tableInfo;
        }
    }

    private static class InnerTableElementsAnalyzer
        extends DefaultTraversalVisitor<Void, ColumnDefinitionContext> {

        @Override
        public Void visitColumnDefinition(ColumnDefinition node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.name(node.ident());
            for (ColumnConstraint columnConstraint : node.constraints()) {
                process(columnConstraint, context);
            }
            if (node.type() != null) {
                process(node.type(), context);
            }
            if (node.generatedExpression() != null) {
                context.analyzedColumnDefinition.generatedExpression(node.generatedExpression());
            }
            return null;
        }

        @Override
        public Void visitAddColumnDefinition(AddColumnDefinition node, ColumnDefinitionContext context) {
            ColumnIdent column = ExpressionToColumnIdentVisitor.convert(node.name());
            context.analyzedColumnDefinition.name(column.name());
            assert context.tableInfo != null : "Table must be available for `addColumnDefinition`";

            // nested columns can only be added using alter table so no other columns exist.
            assert context.analyzedTableElements.columns().size() == 0 :
                "context.analyzedTableElements.columns().size() must be 0";

            final AnalyzedColumnDefinition root = context.analyzedColumnDefinition;
            if (!column.path().isEmpty()) {
                AnalyzedColumnDefinition parent = context.analyzedColumnDefinition;
                AnalyzedColumnDefinition leaf = parent;
                for (String name : column.path()) {
                    parent.dataType(ObjectType.NAME);
                    // Check if parent is already defined.
                    // If it is an array, set the collection type to array, or if it's an object keep the object column
                    // policy.
                    Reference parentRef = context.tableInfo.getReference(parent.ident());
                    if (parentRef != null) {
                        parent.position = parentRef.column().isTopLevel() ? parentRef.position() : null;
                        if (parentRef.valueType().equals(new ArrayType(ObjectType.untyped()))) {
                            parent.collectionType(ArrayType.NAME);
                        } else {
                            parent.objectType(parentRef.columnPolicy());
                        }
                    }
                    parent.markAsParentColumn();
                    leaf = new AnalyzedColumnDefinition(null, parent);
                    leaf.name(name);
                    parent.addChild(leaf);
                    parent = leaf;
                }
                context.analyzedColumnDefinition = leaf;
            }

            for (ColumnConstraint columnConstraint : node.constraints()) {
                process(columnConstraint, context);
            }
            if (node.type() != null) {
                process(node.type(), context);
            }
            if (node.generatedExpression() != null) {
                context.analyzedColumnDefinition.generatedExpression(node.generatedExpression());
            }

            context.analyzedColumnDefinition = root;
            return null;
        }

        @Override
        public Void visitColumnType(ColumnType node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.dataType(node.name());
            return null;
        }

        @Override
        public Void visitObjectColumnType(ObjectColumnType node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.dataType(node.name());
            context.analyzedColumnDefinition.objectType(node.objectType().orElse(ColumnPolicy.DYNAMIC));
            for (int i = 0; i < node.nestedColumns().size(); i++) {
                ColumnDefinition columnDefinition = node.nestedColumns().get(i);
                ColumnDefinitionContext childContext = new ColumnDefinitionContext(
                    null,
                    context.analyzedColumnDefinition,
                    context.parameters,
                    context.fulltextAnalyzerResolver,
                    context.analyzedTableElements,
                    context.relationName,
                    context.tableInfo
                );
                process(columnDefinition, childContext);
                context.analyzedColumnDefinition.addChild(childContext.analyzedColumnDefinition);
            }

            return null;
        }

        @Override
        public Void visitCollectionColumnType(CollectionColumnType node, ColumnDefinitionContext context) {
            if (node.type() == ColumnType.Type.SET) {
                throw new UnsupportedOperationException("the SET dataType is currently not supported");
            }

            context.analyzedColumnDefinition.collectionType(ArrayType.NAME);

            if (node.innerType().type() != ColumnType.Type.PRIMITIVE &&
                node.innerType().type() != ColumnType.Type.OBJECT) {
                throw new UnsupportedOperationException("Nesting ARRAY or SET types is not supported");
            }

            process(node.innerType(), context);
            return null;
        }


        @Override
        public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.setPrimaryKeyConstraint();
            return null;
        }


        @Override
        public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint node, ColumnDefinitionContext context) {
            for (Expression expression : node.columns()) {
                context.analyzedTableElements.addPrimaryKey(
                    ExpressionToStringVisitor.convert(expression, context.parameters));
            }
            return null;
        }

        @Override
        public Void visitIndexColumnConstraint(IndexColumnConstraint node, ColumnDefinitionContext context) {
            if (node.indexMethod().equals("fulltext")) {
                setAnalyzer(node.properties(), context, node.indexMethod());
            } else if (node.indexMethod().equalsIgnoreCase("plain")) {
                context.analyzedColumnDefinition.indexConstraint(Reference.IndexType.NOT_ANALYZED);
            } else if (node.indexMethod().equalsIgnoreCase("OFF")) {
                context.analyzedColumnDefinition.indexConstraint(Reference.IndexType.NO);
            } else if (node.indexMethod().equals("quadtree") || node.indexMethod().equals("geohash")) {
                setGeoType(node.properties(), context, node.indexMethod());
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid index method \"%s\"", node.indexMethod()));
            }
            return null;
        }

        @Override
        public Void visitNotNullColumnConstraint(NotNullColumnConstraint node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.setNotNullConstraint();
            return null;
        }

        @Override
        public Void visitIndexDefinition(IndexDefinition node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.setAsIndexColumn();
            context.analyzedColumnDefinition.dataType("string");
            context.analyzedColumnDefinition.name(node.ident());

            setAnalyzer(node.properties(), context, node.method());

            for (Expression expression : node.columns()) {
                String expressionName = ExpressionToStringVisitor.convert(expression, context.parameters);
                context.analyzedTableElements.addCopyTo(expressionName, node.ident());
            }
            return null;
        }

        @Override
        public Void visitColumnStorageDefinition(ColumnStorageDefinition node, ColumnDefinitionContext context) {
            Settings storageSettings = GenericPropertiesConverter.genericPropertiesToSettings(node.properties(),
                context.parameters);

            for (String property : storageSettings.names()) {
                if (property.equals(COLUMN_STORE_PROPERTY)) {
                    context.analyzedColumnDefinition.setColumnStore(storageSettings.getAsBoolean(property, true));
                } else {
                    throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid storage option \"%s\"", storageSettings.get(property)));
                }
            }
            return null;
        }

        private void setGeoType(GenericProperties properties, ColumnDefinitionContext context, String indexMethod) {
            context.analyzedColumnDefinition.geoTree(indexMethod);
            Settings geoSettings = GenericPropertiesConverter.genericPropertiesToSettings(properties, context.parameters);
            context.analyzedColumnDefinition.geoSettings(geoSettings);
        }

        private void setAnalyzer(GenericProperties properties, ColumnDefinitionContext context,
                                 String indexMethod) {
            context.analyzedColumnDefinition.indexConstraint(Reference.IndexType.ANALYZED);

            Expression analyzerExpression = properties.get("analyzer");
            if (analyzerExpression == null) {
                if (indexMethod.equals("plain")) {
                    context.analyzedColumnDefinition.analyzer("keyword");
                } else {
                    context.analyzedColumnDefinition.analyzer("standard");
                }
                return;
            }
            if (analyzerExpression instanceof ArrayLiteral) {
                throw new IllegalArgumentException("array literal not allowed for the analyzer property");
            }

            String analyzerName = ExpressionToStringVisitor.convert(analyzerExpression, context.parameters);
            if (context.fulltextAnalyzerResolver.hasCustomAnalyzer(analyzerName)) {
                Settings settings = context.fulltextAnalyzerResolver.resolveFullCustomAnalyzerSettings(analyzerName);
                context.analyzedColumnDefinition.analyzerSettings(settings);
            }

            context.analyzedColumnDefinition.analyzer(analyzerName);
        }
    }
}

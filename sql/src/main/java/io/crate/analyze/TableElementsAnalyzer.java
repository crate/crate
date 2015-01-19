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

import io.crate.analyze.expressions.ExpressionToStringVisitor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.ReferenceInfo;
import io.crate.sql.tree.*;
import io.crate.types.DataTypes;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class TableElementsAnalyzer {


    private final static InnerTableElementsAnalyzer analyzer = new InnerTableElementsAnalyzer();

    public static AnalyzedTableElements analyze(List<TableElement> tableElements,
                                                Object[] parameters,
                                                FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        AnalyzedTableElements analyzedTableElements = new AnalyzedTableElements();
        for (TableElement tableElement : tableElements) {
            ColumnDefinitionContext ctx = new ColumnDefinitionContext(
                    null, parameters, fulltextAnalyzerResolver, analyzedTableElements);
            analyzer.process(tableElement, ctx);
            if (ctx.analyzedColumnDefinition.ident() != null) {
                analyzedTableElements.add(ctx.analyzedColumnDefinition);
            }
        }
        return analyzedTableElements;
    }

    public static AnalyzedTableElements analyze(TableElement tableElement,
                                                Object[] parameters,
                                                FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        return analyze(Arrays.asList(tableElement), parameters, fulltextAnalyzerResolver);
    }

    private static class ColumnDefinitionContext {
        final Object[] parameters;
        final FulltextAnalyzerResolver fulltextAnalyzerResolver;
        AnalyzedColumnDefinition analyzedColumnDefinition;
        final AnalyzedTableElements analyzedTableElements;

        public ColumnDefinitionContext(@Nullable AnalyzedColumnDefinition parent,
                                       Object[] parameters,
                                       FulltextAnalyzerResolver fulltextAnalyzerResolver,
                                       AnalyzedTableElements analyzedTableElements) {
            this.analyzedColumnDefinition = new AnalyzedColumnDefinition(parent);
            this.parameters = parameters;
            this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
            this.analyzedTableElements = analyzedTableElements;
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
            process(node.type(), context);
            return null;
        }

        @Override
        public Void visitNestedColumnDefinition(NestedColumnDefinition node, ColumnDefinitionContext context) {
            String columnName = ExpressionToStringVisitor.convert(node.name(), context.parameters);
            ColumnIdent ident = ColumnIdent.fromPath(columnName);
            context.analyzedColumnDefinition.name(ident.name());

            // nested columns can only be added using alter table so no other columns exist.
            assert context.analyzedTableElements.columns().size() == 0;

            AnalyzedColumnDefinition root = context.analyzedColumnDefinition;
            root.dataType(DataTypes.OBJECT.getName());
            if (!ident.path().isEmpty()) {
                AnalyzedColumnDefinition parent = context.analyzedColumnDefinition;
                AnalyzedColumnDefinition leaf = parent;
                for (String name : ident.path()) {
                    parent.dataType(DataTypes.OBJECT.getName());
                    parent.isParentColumn(true);
                    leaf = new AnalyzedColumnDefinition(parent);
                    leaf.name(name);
                    parent.addChild(leaf);
                    parent = leaf;
                }
                context.analyzedColumnDefinition = leaf;
            }

            for (ColumnConstraint columnConstraint : node.constraints()) {
                process(columnConstraint, context);
            }
            process(node.type(), context);
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

            switch (node.objectType().or("dynamic").toLowerCase(Locale.ENGLISH)) {
                case "dynamic":
                    context.analyzedColumnDefinition.objectType("true");
                    break;
                case "strict":
                    context.analyzedColumnDefinition.objectType("strict");
                    break;
                case "ignored":
                    context.analyzedColumnDefinition.objectType("false");
                    break;
            }

            for (ColumnDefinition columnDefinition : node.nestedColumns()) {
                ColumnDefinitionContext childContext = new ColumnDefinitionContext(
                        context.analyzedColumnDefinition,
                        context.parameters,
                        context.fulltextAnalyzerResolver,
                        context.analyzedTableElements
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

            context.analyzedColumnDefinition.collectionType("array");

            if (node.innerType().type() != ColumnType.Type.PRIMITIVE) {
                throw new UnsupportedOperationException("Nesting ARRAY or SET types is not supported");
            }

            process(node.innerType(), context);
            return null;
        }


        @Override
        public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.isPrimaryKey(true);
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
            if (node.indexMethod().equalsIgnoreCase("fulltext")) {
                setAnalyzer(node.properties(), context, node.indexMethod());
            } else if (node.indexMethod().equalsIgnoreCase("plain")) {
                context.analyzedColumnDefinition.index(ReferenceInfo.IndexType.NOT_ANALYZED.toString());
            } else if (node.indexMethod().equalsIgnoreCase("OFF")) {
                context.analyzedColumnDefinition.index(ReferenceInfo.IndexType.NO.toString());
            } else {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid index method \"%s\"", node.indexMethod()));
            }
            return null;
        }


        @Override
        public Void visitIndexDefinition(IndexDefinition node, ColumnDefinitionContext context) {
            context.analyzedColumnDefinition.isIndex(true);
            context.analyzedColumnDefinition.dataType("string");
            context.analyzedColumnDefinition.name(node.ident());

            setAnalyzer(node.properties(), context, node.method());

            for (Expression expression : node.columns()) {
                String expressionName = ExpressionToStringVisitor.convert(expression, context.parameters);
                context.analyzedTableElements.addCopyTo(expressionName, node.ident());
            }
            return null;
        }

        private void setAnalyzer(GenericProperties properties, ColumnDefinitionContext context,
                                 String indexMethod) {
            context.analyzedColumnDefinition.index(ReferenceInfo.IndexType.ANALYZED.toString());

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

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.AddColumnDefinition;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.DropCheckConstraint;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.TableElement;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class TableElementsAnalyzer {

    /**
     *
     * @param isAddColumn When set to true, column positions of the analyzed table elements will contain negative column estimates
     *                    representing the ordering of the columns to be added dynamically. The estimates will be assigned by {@link ColumnDefinitionContext#increaseCurrentPosition()}
     *                    then re-calculated to be the exact column positions by {@link org.elasticsearch.cluster.metadata.ColumnPositionResolver}
     */
    public static <T> AnalyzedTableElements<T> analyze(List<? extends TableElement<T>> tableElements,
                                                       RelationName relationName,
                                                       @Nullable TableInfo tableInfo,
                                                       boolean isAddColumn) {
        AnalyzedTableElements<T> analyzedTableElements = new AnalyzedTableElements<>();
        InnerTableElementsAnalyzer<T> analyzer = new InnerTableElementsAnalyzer<>();
        for (int i = 0; i < tableElements.size(); i++) {
            TableElement<T> tableElement = tableElements.get(i);
            ColumnDefinitionContext<T> ctx = new ColumnDefinitionContext<>(
                null,
                analyzedTableElements,
                relationName,
                tableInfo
            );

            tableElement.accept(analyzer, ctx);
            if (ctx.analyzedColumnDefinition.ident() != null) {
                analyzedTableElements.add(ctx.analyzedColumnDefinition, isAddColumn);
            }
        }
        return analyzedTableElements;
    }

    private static class ColumnDefinitionContext<T> {

        AnalyzedColumnDefinition<T> analyzedColumnDefinition;
        final AnalyzedTableElements<T> analyzedTableElements;
        final RelationName relationName;
        @Nullable
        final TableInfo tableInfo;

        ColumnDefinitionContext(@Nullable AnalyzedColumnDefinition<T> parent,
                                AnalyzedTableElements<T> analyzedTableElements,
                                RelationName relationName,
                                @Nullable TableInfo tableInfo) {
            this.analyzedColumnDefinition = new AnalyzedColumnDefinition<>(parent);
            this.analyzedTableElements = analyzedTableElements;
            this.relationName = relationName;
            this.tableInfo = tableInfo;
        }
    }

    private static class InnerTableElementsAnalyzer<T> extends DefaultTraversalVisitor<Void, ColumnDefinitionContext<T>> {

        @Override
        @SuppressWarnings("unchecked")
        public Void visitColumnDefinition(ColumnDefinition<?> node, ColumnDefinitionContext<T> context) {
            ColumnDefinition<T> columnDefinition = (ColumnDefinition<T>) node;
            context.analyzedColumnDefinition.name(node.ident());
            for (ColumnConstraint<T> columnConstraint : columnDefinition.constraints()) {
                columnConstraint.accept(this, context);
            }
            ColumnType<T> type = columnDefinition.type();
            if (type != null) {
                type.accept(this, context);
            }
            context.analyzedColumnDefinition.setGenerated(columnDefinition.isGenerated());
            if (columnDefinition.defaultExpression() != null) {
                context.analyzedColumnDefinition.defaultExpression(columnDefinition.defaultExpression());
            }
            if (columnDefinition.generatedExpression() != null) {
                context.analyzedColumnDefinition.generatedExpression(columnDefinition.generatedExpression());
            }
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitAddColumnDefinition(AddColumnDefinition<?> node, ColumnDefinitionContext<T> context) {
            AddColumnDefinition<T> addColumnDefinition = (AddColumnDefinition<T>) node;
            assert addColumnDefinition.name() instanceof Symbol : "column name is expected to be a symbol already";
            ColumnIdent column = Symbols.pathFromSymbol((Symbol) addColumnDefinition.name());
            context.analyzedColumnDefinition.name(column.name());

            assert context.tableInfo != null : "Table must be available for `addColumnDefinition`";

            final AnalyzedColumnDefinition<T> root = context.analyzedColumnDefinition;
            if (!column.path().isEmpty()) {
                AnalyzedColumnDefinition<T> parent = context.analyzedColumnDefinition;
                AnalyzedColumnDefinition<T> leaf = parent;
                for (String name : column.path()) {
                    // Check if parent is already defined.
                    Reference parentRef = context.tableInfo.getReference(parent.ident());
                    if (parentRef == null) {
                        parent.dataType(DataTypes.UNTYPED_OBJECT);
                    } else {
                        parent.dataType(parentRef.valueType());
                        parent.columnPolicy(parentRef.columnPolicy());
                    }
                    parent.markAsParentColumn();
                    leaf = new AnalyzedColumnDefinition<>(parent);
                    leaf.name(name);
                    parent.addChild(leaf);
                    parent = leaf;
                }
                context.analyzedColumnDefinition = leaf;
            }

            for (ColumnConstraint<T> columnConstraint : addColumnDefinition.constraints()) {
                columnConstraint.accept(this, context);
            }
            ColumnType<?> type = node.type();
            if (type != null) {
                type.accept(this, context);
            }
            context.analyzedColumnDefinition.setGenerated(addColumnDefinition.isGenerated());
            if (addColumnDefinition.generatedExpression() != null) {
                context.analyzedColumnDefinition.generatedExpression(addColumnDefinition.generatedExpression());
            }

            context.analyzedColumnDefinition = root;
            return null;
        }

        @Override
        public Void visitColumnType(ColumnType<?> node, ColumnDefinitionContext<T> context) {
            DataType<?> type = DataTypeAnalyzer.convert(node);
            context.analyzedColumnDefinition.dataType(type);
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitObjectColumnType(ObjectColumnType<?> node, ColumnDefinitionContext<T> context) {
            ObjectColumnType<T> objectColumnType = (ObjectColumnType<T>) node;
            DataType<?> type = DataTypeAnalyzer.convert(node);
            context.analyzedColumnDefinition.dataType(type);
            context.analyzedColumnDefinition.columnPolicy(objectColumnType.columnPolicy().orElse(ColumnPolicy.DYNAMIC));
            for (int i = 0; i < objectColumnType.nestedColumns().size(); i++) {
                ColumnDefinition<T> columnDefinition = objectColumnType.nestedColumns().get(i);
                ColumnDefinitionContext<T> childContext = new ColumnDefinitionContext<>(
                    context.analyzedColumnDefinition,
                    context.analyzedTableElements,
                    context.relationName,
                    context.tableInfo
                );
                columnDefinition.accept(this, childContext);
                context.analyzedColumnDefinition.addChild(childContext.analyzedColumnDefinition);
            }

            return null;
        }

        @Override
        public Void visitCollectionColumnType(CollectionColumnType<?> node, ColumnDefinitionContext<T> context) {
            DataType<?> type = DataTypeAnalyzer.convert(node);
            node.innerType().accept(this, context);
            context.analyzedColumnDefinition.dataType(type);
            return null;
        }


        @Override
        public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedColumnDefinition.setPrimaryKeyConstraint();
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitPrimaryKeyConstraint(PrimaryKeyConstraint<?> node, ColumnDefinitionContext<T> context) {
            PrimaryKeyConstraint<T> primaryKeyConstraint = (PrimaryKeyConstraint<T>) node;
            for (T name : primaryKeyConstraint.columns()) {
                context.analyzedTableElements.addPrimaryKey(name);
            }
            return null;
        }

        @Override
        public Void visitCheckConstraint(CheckConstraint<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedTableElements.addCheckConstraint(context.relationName, node);
            return null;
        }

        @Override
        public Void visitCheckColumnConstraint(CheckColumnConstraint<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedTableElements.addCheckColumnConstraint(context.relationName, node);
            return null;
        }

        @Override
        public Void visitDropCheckConstraint(DropCheckConstraint<?> node, ColumnDefinitionContext<T> context) {
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitIndexColumnConstraint(IndexColumnConstraint<?> node, ColumnDefinitionContext<T> context) {
            if (node.indexMethod().equals("fulltext")) {
                setAnalyzer((GenericProperties<T>) node.properties(), context, node.indexMethod());
            } else if (node.indexMethod().equalsIgnoreCase("plain")) {
                context.analyzedColumnDefinition.indexType(IndexType.PLAIN);
            } else if (node.indexMethod().equalsIgnoreCase("OFF")) {
                context.analyzedColumnDefinition.indexType(IndexType.NONE);
            } else if (node.indexMethod().equals("quadtree") || node.indexMethod().equals("geohash")) {
                context.analyzedColumnDefinition.geoTree(node.indexMethod());
                context.analyzedColumnDefinition.geoProperties((GenericProperties<T>) node.properties());
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid index method \"%s\"", node.indexMethod()));
            }
            return null;
        }

        @Override
        public Void visitNotNullColumnConstraint(NotNullColumnConstraint<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedColumnDefinition.setNotNullConstraint();
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitIndexDefinition(IndexDefinition<?> node, ColumnDefinitionContext<T> context) {
            IndexDefinition<T> indexDefinition = (IndexDefinition<T>) node;
            context.analyzedColumnDefinition.setAsIndexColumn();
            context.analyzedColumnDefinition.dataType(DataTypes.STRING);
            context.analyzedColumnDefinition.name(indexDefinition.ident());

            setAnalyzer(indexDefinition.properties(), context, indexDefinition.method());

            Set<T> uniqueColumns = new HashSet<>(indexDefinition.columns());
            if (uniqueColumns.size() != indexDefinition.columns().size()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Index %s contains duplicate columns.", indexDefinition.ident())
                );
            }
            for (T symbol : indexDefinition.columns()) {
                context.analyzedTableElements.addFTSource(
                    symbol,
                    indexDefinition.ident());
            }
            return null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Void visitColumnStorageDefinition(ColumnStorageDefinition<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedColumnDefinition.setStorageProperties((GenericProperties<T>) node.properties());
            return null;
        }

        private void setAnalyzer(GenericProperties<T> properties,
                                 ColumnDefinitionContext<T> context,
                                 String indexMethod) {
            context.analyzedColumnDefinition.indexType(IndexType.FULLTEXT);

            T analyzerName = properties.get("analyzer");
            if (analyzerName == null) {
                context.analyzedColumnDefinition.indexMethod(indexMethod);
                return;
            }
            context.analyzedColumnDefinition.analyzer(analyzerName);
        }
    }
}

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

import io.crate.expression.symbol.Literal;
import io.crate.metadata.ColumnIdent;
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
import io.crate.types.ArrayType;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;

public class TableElementsAnalyzer {

    public static <T> AnalyzedTableElements<T> analyze(List<TableElement<T>> tableElements,
                                                       RelationName relationName,
                                                       @Nullable TableInfo tableInfo) {
        return analyze(tableElements, relationName, tableInfo, true);
    }

    public static <T> AnalyzedTableElements<T> analyze(List<TableElement<T>> tableElements,
                                                       RelationName relationName,
                                                       @Nullable TableInfo tableInfo,
                                                       boolean logWarnings) {
        AnalyzedTableElements<T> analyzedTableElements = new AnalyzedTableElements<>();
        int positionOffset = tableInfo == null ? 0 : tableInfo.columns().size();
        InnerTableElementsAnalyzer<T> analyzer = new InnerTableElementsAnalyzer<>();
        for (int i = 0; i < tableElements.size(); i++) {
            TableElement<T> tableElement = tableElements.get(i);
            int position = positionOffset + 1;
            ColumnDefinitionContext<T> ctx = new ColumnDefinitionContext<>(
                position,
                null,
                analyzedTableElements,
                relationName,
                tableInfo,
                logWarnings);

            tableElement.accept(analyzer, ctx);
            if (ctx.analyzedColumnDefinition.ident() != null) {
                analyzedTableElements.add(ctx.analyzedColumnDefinition);
            }
            positionOffset = ctx.currentColumnPosition;
        }
        return analyzedTableElements;
    }

    private static class ColumnDefinitionContext<T> {

        AnalyzedColumnDefinition<T> analyzedColumnDefinition;
        final AnalyzedTableElements<T> analyzedTableElements;
        final RelationName relationName;
        @Nullable
        final TableInfo tableInfo;
        final boolean logWarnings;
        int currentColumnPosition;

        ColumnDefinitionContext(int position,
                                @Nullable AnalyzedColumnDefinition<T> parent,
                                AnalyzedTableElements<T> analyzedTableElements,
                                RelationName relationName,
                                @Nullable TableInfo tableInfo,
                                boolean logWarnings) {
            this.analyzedColumnDefinition = new AnalyzedColumnDefinition<>(position, parent);
            this.analyzedTableElements = analyzedTableElements;
            this.relationName = relationName;
            this.tableInfo = tableInfo;
            this.logWarnings = logWarnings;
            this.currentColumnPosition = position;
        }

        public void increaseCurrentPosition() {
            currentColumnPosition++;
        }
    }

    private static class InnerTableElementsAnalyzer<T> extends DefaultTraversalVisitor<Void, ColumnDefinitionContext<T>> {

        @Override
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
        public Void visitAddColumnDefinition(AddColumnDefinition<?> node, ColumnDefinitionContext<T> context) {
            AddColumnDefinition<T> addColumnDefinition = (AddColumnDefinition<T>) node;
            assert addColumnDefinition.name() instanceof Literal : "column name is expected to be a literal already";
            ColumnIdent column = ColumnIdent.fromPath(((Literal) addColumnDefinition.name()).value().toString());
            context.analyzedColumnDefinition.name(column.name());

            assert context.tableInfo != null : "Table must be available for `addColumnDefinition`";

            // nested columns can only be added using alter table so no other columns exist.
            assert context.analyzedTableElements.columns().size() == 0 :
                "context.analyzedTableElements.columns().size() must be 0";

            final AnalyzedColumnDefinition<T> root = context.analyzedColumnDefinition;
            if (!column.path().isEmpty()) {
                AnalyzedColumnDefinition<T> parent = context.analyzedColumnDefinition;
                AnalyzedColumnDefinition<T> leaf = parent;
                for (String name : column.path()) {
                    parent.dataType(ObjectType.NAME);
                    // Check if parent is already defined.
                    // If it is an array, set the collection type to array, or if it's an object keep the object column
                    // policy.
                    Reference parentRef = context.tableInfo.getReference(parent.ident());
                    int childrenCnt = 0;
                    if (parentRef != null) {
                        parent.position = parentRef.position();
                        if (parentRef.valueType().id() == ArrayType.ID) {
                            parent.collectionType(ArrayType.NAME);
                        } else {
                            childrenCnt = ((ObjectType) parentRef.valueType()).innerTypes().size();
                            parent.objectType(parentRef.columnPolicy());
                        }
                    }
                    parent.markAsParentColumn();
                    int position = parent.position + childrenCnt + 1;
                    context.currentColumnPosition = position;
                    leaf = new AnalyzedColumnDefinition<>(position, parent);
                    leaf.name(name);
                    parent.addChild(leaf);
                    parent = leaf;
                }
                context.analyzedColumnDefinition = leaf;
            }

            for (ColumnConstraint<T> columnConstraint : addColumnDefinition.constraints()) {
                columnConstraint.accept(this, context);
            }
            ColumnType type = node.type();
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
            context.analyzedColumnDefinition.dataType(node.name(), node.parameters(), context.logWarnings);
            return null;
        }

        @Override
        public Void visitObjectColumnType(ObjectColumnType<?> node, ColumnDefinitionContext<T> context) {
            ObjectColumnType<T> objectColumnType = (ObjectColumnType<T>) node;
            context.analyzedColumnDefinition.dataType(objectColumnType.name());
            context.analyzedColumnDefinition.objectType(objectColumnType.objectType().orElse(ColumnPolicy.DYNAMIC));
            for (int i = 0; i < objectColumnType.nestedColumns().size(); i++) {
                ColumnDefinition<T> columnDefinition = objectColumnType.nestedColumns().get(i);
                context.increaseCurrentPosition();
                ColumnDefinitionContext<T> childContext = new ColumnDefinitionContext<>(
                    context.currentColumnPosition,
                    context.analyzedColumnDefinition,
                    context.analyzedTableElements,
                    context.relationName,
                    context.tableInfo,
                    context.logWarnings
                );
                columnDefinition.accept(this, childContext);
                context.analyzedColumnDefinition.addChild(childContext.analyzedColumnDefinition);
            }

            return null;
        }

        @Override
        public Void visitCollectionColumnType(CollectionColumnType<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedColumnDefinition.collectionType(ArrayType.NAME);

            if (node.innerType() instanceof CollectionColumnType) {
                throw new UnsupportedOperationException("Nesting ARRAY or SET types is not supported");
            }

            node.innerType().accept(this, context);
            return null;
        }


        @Override
        public Void visitPrimaryKeyColumnConstraint(PrimaryKeyColumnConstraint<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedColumnDefinition.setPrimaryKeyConstraint();
            return null;
        }


        @Override
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
        public Void visitIndexColumnConstraint(IndexColumnConstraint<?> node, ColumnDefinitionContext<T> context) {
            if (node.indexMethod().equals("fulltext")) {
                setAnalyzer((GenericProperties<T>) node.properties(), context, node.indexMethod());
            } else if (node.indexMethod().equalsIgnoreCase("plain")) {
                context.analyzedColumnDefinition.indexConstraint(Reference.IndexType.NOT_ANALYZED);
            } else if (node.indexMethod().equalsIgnoreCase("OFF")) {
                context.analyzedColumnDefinition.indexConstraint(Reference.IndexType.NO);
            } else if (node.indexMethod().equals("quadtree") || node.indexMethod().equals("geohash")) {
                setGeoType((GenericProperties<T>) node.properties(), context, node.indexMethod());
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
        public Void visitIndexDefinition(IndexDefinition<?> node, ColumnDefinitionContext<T> context) {
            IndexDefinition<T> indexDefinition = (IndexDefinition<T>) node;
            context.analyzedColumnDefinition.setAsIndexColumn();
            context.analyzedColumnDefinition.dataType("string");
            context.analyzedColumnDefinition.name(indexDefinition.ident());

            setAnalyzer(indexDefinition.properties(), context, indexDefinition.method());

            for (T symbol : indexDefinition.columns()) {
                context.analyzedTableElements.addCopyTo(
                    symbol,
                    indexDefinition.ident());
            }
            return null;
        }

        @Override
        public Void visitColumnStorageDefinition(ColumnStorageDefinition<?> node, ColumnDefinitionContext<T> context) {
            context.analyzedColumnDefinition.setStorageProperties((GenericProperties<T>) node.properties());
            return null;
        }

        private void setGeoType(GenericProperties<T> properties, ColumnDefinitionContext<T> context, String indexMethod) {
            context.analyzedColumnDefinition.geoTree(indexMethod);
            context.analyzedColumnDefinition.geoProperties(properties);
        }

        private void setAnalyzer(GenericProperties<T> properties,
                                 ColumnDefinitionContext<T> context,
                                 String indexMethod) {
            context.analyzedColumnDefinition.indexConstraint(Reference.IndexType.ANALYZED);

            T analyzerName = properties.get("analyzer");
            if (analyzerName == null) {
                context.analyzedColumnDefinition.indexMethod(indexMethod);
                return;
            }
            context.analyzedColumnDefinition.analyzer(analyzerName);
        }
    }
}

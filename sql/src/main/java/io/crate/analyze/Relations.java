/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze;

import com.google.common.collect.Lists;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.OrderedLimitedRelation;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Functions;
import io.crate.metadata.Path;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.sql.tree.QualifiedName;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public class Relations {

    static Collection<? extends Path> namesFromOutputs(List<Symbol> outputs) {
        return Lists.transform(outputs, Symbols::pathFromSymbol);
    }

    /**
     * Promotes the relation to a QueriedRelation by applying the QuerySpec.
     *
     * <pre>
     * TableRelation -> QueriedTable
     * QueriedTable  -> QueriedSelect
     * QueriedSelect -> nested QueriedSelect
     * </pre>
     *
     * If the result is a QueriedTable it is also normalized.
     */
    static QueriedRelation applyQSToRelation(RelationNormalizer normalizer,
                                             Functions functions,
                                             TransactionContext transactionContext,
                                             AnalyzedRelation relation,
                                             QuerySpec querySpec) {
        QueriedRelation newRelation;
        if (relation instanceof AbstractTableRelation) {
            AbstractTableRelation<?> tableRelation = (AbstractTableRelation<?>) relation;
            EvaluatingNormalizer evalNormalizer = new EvaluatingNormalizer(
                functions, RowGranularity.CLUSTER, null, tableRelation);

            newRelation = new QueriedTable<>(
                tableRelation, querySpec.copyAndReplace(s -> evalNormalizer.normalize(s, transactionContext)));
        } else {
            newRelation = new QueriedSelectRelation(
                ((QueriedRelation) relation), namesFromOutputs(querySpec.outputs()), querySpec);
            newRelation = (QueriedRelation) normalizer.normalize(newRelation, transactionContext);
        }
        if (newRelation.where().hasQuery() && newRelation.getQualifiedName().equals(relation.getQualifiedName())) {
            // This relation will be represented as a subquery and needs a proper QualifiedName.
            // If there is no distinct QualifiedName, create one by merging all parts of the old QualifiedName
            // e.g. "doc"."users" -> "doc.users"
            newRelation.setQualifiedName(QualifiedName.of(relation.getQualifiedName().toString()));
        } else {
            newRelation.setQualifiedName(relation.getQualifiedName());
        }
        return newRelation;
    }

    /**
     * Calls the consumer on all symbols of the given statement.
     * Traverses into all sub-relations
     */
    public static void traverseDeepSymbols(AnalyzedStatement stmt, Consumer<? super Symbol> consumer) {
        TraverseDeepSymbolsStatements.traverse(stmt, consumer);
    }

    private static class TraverseDeepSymbolsStatements extends AnalyzedStatementVisitor<Consumer<? super Symbol>, Void> {

        private static final TraverseDeepSymbolsStatements INSTANCE = new TraverseDeepSymbolsStatements();

        static void traverse(AnalyzedStatement stmt, Consumer<? super Symbol> consumer) {
            INSTANCE.process(stmt, consumer);
        }

        @Override
        public Void visitInsert(AnalyzedInsertStatement insert, Consumer<? super Symbol> consumer) {
            List<List<Symbol>> rows = insert.rows();
            if (rows != null) {
                for (List<Symbol> row : rows) {
                    row.forEach(consumer);
                }
            }
            AnalyzedStatement subRelation = insert.subRelation();
            if (subRelation != null) {
                traverseDeepSymbols(subRelation, consumer);
            }
            insert.onDuplicateKeyAssignments().values().forEach(consumer);
            return null;
        }

        @Override
        public Void visitSelectStatement(QueriedRelation relation, Consumer<? super Symbol> consumer) {
            TraverseDeepSymbolsRelations.traverse(relation, consumer);
            return null;
        }

        @Override
        protected Void visitAnalyzedStatement(AnalyzedStatement analyzedStatement, Consumer<? super Symbol> consumer) {
            analyzedStatement.visitSymbols(consumer);
            return null;
        }
    }

    private static class TraverseDeepSymbolsRelations extends AnalyzedRelationVisitor<Consumer<? super Symbol>, Void> {

        private static final TraverseDeepSymbolsRelations INSTANCE = new TraverseDeepSymbolsRelations();

        static void traverse(AnalyzedRelation relation, Consumer<? super Symbol> consumer) {
            INSTANCE.process(relation, consumer);
        }

        @Override
        public Void visitQueriedTable(QueriedTable<?> queriedTable, Consumer<? super Symbol> consumer) {
            queriedTable.visitSymbols(consumer);
            process(queriedTable.tableRelation(), consumer);
            return null;
        }

        @Override
        public Void visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Consumer<? super Symbol> consumer) {
            multiSourceSelect.visitSymbols(consumer);
            for (AnalyzedRelation relation : multiSourceSelect.sources().values()) {
                process(relation, consumer);
            }
            return null;
        }

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, Consumer<? super Symbol> consumer) {
            unionSelect.visitSymbols(consumer);
            process(unionSelect.left(), consumer);
            process(unionSelect.right(), consumer);
            return null;
        }

        @Override
        public Void visitTableRelation(TableRelation tableRelation, Consumer<? super Symbol> consumer) {
            return null;
        }

        @Override
        public Void visitDocTableRelation(DocTableRelation relation, Consumer<? super Symbol> consumer) {
            return null;
        }

        @Override
        public Void visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, Consumer<? super Symbol> consumer) {
            for (Symbol argument : tableFunctionRelation.function().arguments()) {
                consumer.accept(argument);
            }
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, Consumer<? super Symbol> consumer) {
            relation.visitSymbols(consumer);
            process(relation.subRelation(), consumer);
            return null;
        }

        @Override
        public Void visitOrderedLimitedRelation(OrderedLimitedRelation relation, Consumer<? super Symbol> consumer) {
            relation.visitSymbols(consumer);
            process(relation.childRelation(), consumer);
            return null;
        }

        @Override
        public Void visitView(AnalyzedView analyzedView, Consumer<? super Symbol> consumer) {
            analyzedView.visitSymbols(consumer);
            process(analyzedView.relation(), consumer);
            return null;
        }
    }
}

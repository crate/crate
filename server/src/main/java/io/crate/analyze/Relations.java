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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.expression.symbol.Symbol;

import java.util.function.Consumer;

public class Relations {

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
            stmt.accept(INSTANCE, consumer);
        }

        @Override
        public Void visitSelectStatement(AnalyzedRelation relation, Consumer<? super Symbol> consumer) {
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
            relation.accept(INSTANCE, consumer);
        }

        @Override
        public Void visitUnionSelect(UnionSelect unionSelect, Consumer<? super Symbol> consumer) {
            unionSelect.visitSymbols(consumer);
            unionSelect.left().accept(this, consumer);
            unionSelect.right().accept(this, consumer);
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
            for (AnalyzedRelation analyzedRelation : relation.from()) {
                analyzedRelation.accept(this, consumer);
            }
            return null;
        }

        @Override
        public Void visitJoinRelation(JoinRelation join, Consumer<? super Symbol> consumer) {
            join.visitSymbols(consumer);
            join.left().accept(this, consumer);
            join.right().accept(this, consumer);
            return null;
        }

        @Override
        public Void visitView(AnalyzedView analyzedView, Consumer<? super Symbol> consumer) {
            analyzedView.visitSymbols(consumer);
            analyzedView.relation().accept(this, consumer);
            return null;
        }
    }
}

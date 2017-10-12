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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.metadata.TransactionContext;
import io.crate.sql.SqlFormatter;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Explain;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.ShowColumns;
import io.crate.sql.tree.ShowCreateTable;
import io.crate.sql.tree.ShowSchemas;
import io.crate.sql.tree.ShowTables;
import io.crate.sql.tree.ShowTransaction;
import io.crate.sql.tree.Statement;

/**
 * Analyzer that can analyze statements without having access to the Parameters/ParameterContext.
 * <p>
 *     Currently this only works for select statements and the analysis cannot be re-used because it contains mutable components
 *     (like QuerySpec & the symbols within)
 * </p>
 */
class UnboundAnalyzer {

    private final UnboundDispatcher dispatcher;

    UnboundAnalyzer(RelationAnalyzer relationAnalyzer,
                    ShowCreateTableAnalyzer showCreateTableAnalyzer,
                    ShowStatementAnalyzer showStatementAnalyzer) {
        this.dispatcher = new UnboundDispatcher(relationAnalyzer, showCreateTableAnalyzer, showStatementAnalyzer);
    }

    public AnalyzedRelation analyze(Statement statement, SessionContext sessionContext, ParamTypeHints paramTypeHints) {
        return dispatcher.process(statement, new Analysis(sessionContext, ParameterContext.EMPTY, paramTypeHints));
    }

    private static class UnboundDispatcher extends AstVisitor<AnalyzedRelation, Analysis> {

        private final RelationAnalyzer relationAnalyzer;
        private final ShowCreateTableAnalyzer showCreateTableAnalyzer;
        private final ShowStatementAnalyzer showStatementAnalyzer;

        UnboundDispatcher(RelationAnalyzer relationAnalyzer,
                          ShowCreateTableAnalyzer showCreateTableAnalyzer,
                          ShowStatementAnalyzer showStatementAnalyzer) {
            this.relationAnalyzer = relationAnalyzer;
            this.showCreateTableAnalyzer = showCreateTableAnalyzer;
            this.showStatementAnalyzer = showStatementAnalyzer;
        }

        @Override
        protected AnalyzedRelation visitQuery(Query node, Analysis context) {
            return relationAnalyzer.analyzeUnbound(node, context.transactionContext(), context.paramTypeHints());
        }

        @Override
        public AnalyzedRelation visitShowTransaction(ShowTransaction showTransaction, Analysis context) {
            Query query = showStatementAnalyzer.rewriteShowTransaction();
            return relationAnalyzer.analyzeUnbound(query, context.transactionContext(), ParamTypeHints.EMPTY);
        }

        @Override
        protected AnalyzedRelation visitShowTables(ShowTables node, Analysis context) {
            ParameterContext parameterContext = context.parameterContext();
            Query query = showStatementAnalyzer.rewriteShowTables(node);
            return relationAnalyzer.analyzeUnbound(query, context.transactionContext(), parameterContext.typeHints());
        }

        @Override
        protected AnalyzedRelation visitShowSchemas(ShowSchemas node, Analysis context) {
            Query query = showStatementAnalyzer.rewriteShowSchemas(node);
            return relationAnalyzer.analyzeUnbound(query,
                context.transactionContext(),
                context.parameterContext().typeHints());
        }

        @Override
        protected AnalyzedRelation visitShowColumns(ShowColumns node, Analysis context) {
            TransactionContext transactionContext = context.transactionContext();
            Query query = showStatementAnalyzer.rewriteShowColumns(node,
                transactionContext.sessionContext().defaultSchema());
            return relationAnalyzer.analyzeUnbound(query, transactionContext, context.parameterContext().typeHints());
        }

        @Override
        public AnalyzedRelation visitShowCreateTable(ShowCreateTable node, Analysis context) {
            return showCreateTableAnalyzer.analyze(node.table(), context.sessionContext());
        }

        @Override
        protected AnalyzedRelation visitExplain(Explain node, Analysis context) {
            // Sub-relation is ignored for now.
            // This is because the sub-relation might be anything and this unbound analyzer only supports select/show queries

            // this only works because the result here is only used for the Describe message.
            // Once this analysis is used for more this has to be extended
            return new ExplainAnalyzedStatement(SqlFormatter.formatSql(node), null);
        }
    }
}

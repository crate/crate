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

package io.crate.planner;

import java.util.ArrayList;
import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.CreateViewStmt;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.exceptions.RelationAlreadyExists;
import io.crate.execution.ddl.views.CreateViewRequest;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.SearchPath;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.SqlFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.Table;
import io.crate.role.Role;

public final class CreateViewPlan implements Plan {

    private final CreateViewStmt createViewStmt;

    CreateViewPlan(CreateViewStmt createViewStmt) {
        this.createViewStmt = createViewStmt;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) {

        Role owner = createViewStmt.owner();
        String formattedQuery = SqlFormatter.formatSql(createViewStmt.query(), makeExpressions(params));
        ensureFormattedQueryCanStillBeAnalyzed(
            createViewStmt.name(),
            dependencies.nodeContext(),
            dependencies.schemas(),
            plannerContext.transactionContext(),
            formattedQuery,
            createViewStmt.replaceExisting()
        );
        CreateViewRequest request = new CreateViewRequest(
            createViewStmt.name(),
            formattedQuery,
            createViewStmt.replaceExisting(),
            plannerContext.transactionContext().sessionSettings().searchPath(),
            owner == null ? null : owner.name()
        );
        dependencies.createViewAction().execute(request).whenComplete(new OneRowActionListener<>(consumer, resp -> {
            if (resp.alreadyExistsFailure()) {
                throw new RelationAlreadyExists(createViewStmt.name());
            }
            return new Row1(1L);
        }));
    }

    private static void ensureFormattedQueryCanStillBeAnalyzed(RelationName viewName,
                                                               NodeContext nodeCtx,
                                                               Schemas schemas,
                                                               CoordinatorTxnCtx txnCtx,
                                                               String formattedQuery,
                                                               boolean replaceExisting) {
        RelationAnalyzer analyzer = new RelationAnalyzer(nodeCtx, schemas);
        Query query = (Query) SqlParser.createStatement(formattedQuery);
        if (replaceExisting) {
            new EnsureNoSelfReference(viewName, txnCtx.sessionSettings().searchPath())
                .raiseOnSelfReference(query);
        }
        analyzer.analyze(query, txnCtx, new ParamTypeHints(List.of()) {

                @Override
                public Symbol apply(@Nullable ParameterExpression input) {
                    throw new UnsupportedOperationException(
                        "View definition must not contain any parameter placeholders");
                }
            }
        );
    }

    private static List<Expression> makeExpressions(Row params) {
        ArrayList<Expression> result = new ArrayList<>(params.numColumns());
        for (int i = 0; i < params.numColumns(); i++) {
            result.add(Literal.fromObject(params.get(i)));
        }
        return result;
    }


    static class EnsureNoSelfReference extends DefaultTraversalVisitor<Void, Void> {

        private final RelationName viewName;
        private final SearchPath searchPath;

        public EnsureNoSelfReference(RelationName viewName, SearchPath searchPath) {
            this.viewName = viewName;
            this.searchPath = searchPath;
        }

        public void raiseOnSelfReference(Query query) {
            query.accept(this, null);
        }

        @Override
        protected Void visitTable(Table<?> node, Void context) {
            RelationName tableName = RelationName.of(node.getName(), searchPath.currentSchema());
            if (tableName.equals(viewName)) {
                throw new IllegalArgumentException("Creating a view that references itself is not allowed. (`" + viewName + "`)");
            }
            return null;
        }
    }
}

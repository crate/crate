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

package io.crate.action.sql;

import io.crate.action.FutureActionListener;
import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.CreateIngestionRuleAnalysedStatement;
import io.crate.analyze.DropIngestionRuleAnalysedStatement;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.PrivilegesAnalyzedStatement;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.concurrent.CompletableFutures;
import io.crate.data.Row;
import io.crate.exceptions.SQLExceptions;
import io.crate.metadata.rule.ingest.IngestRule;
import io.crate.ingestion.CreateIngestRuleRequest;
import io.crate.ingestion.DropIngestRuleRequest;
import io.crate.ingestion.IngestRuleResponse;
import io.crate.ingestion.TransportCreateIngestRuleAction;
import io.crate.ingestion.TransportDropIngestRuleAction;
import io.crate.auth.user.UserManager;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.ParameterExpression;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Visitor that dispatches requests based on Analysis class to different DCL actions.
 * <p>
 * Its methods return a future returning a Long containing the response rowCount.
 */
@Singleton
public class DCLStatementDispatcher implements BiFunction<AnalyzedStatement, Row, CompletableFuture<Long>> {


    private static class ExpressionParameterSubstitutionFormatter extends ExpressionFormatter.Formatter {

        private final ParameterContext parameterContext;
        private final SymbolPrinter symbolPrinter;

        ExpressionParameterSubstitutionFormatter(SymbolPrinter symbolPrinter, ParameterContext context) {
            this.symbolPrinter = symbolPrinter;
            this.parameterContext = context;
        }

        @Override
        public String visitParameterExpression(ParameterExpression node, Void context) {
            String formattedExpression = null;
            Symbol symbol = parameterContext.apply(node);
            if (symbol != null) {
                formattedExpression = symbolPrinter.printSimple(symbol);
            }
            return formattedExpression;
        }
    }

    private final InnerVisitor INNER_VISITOR = new InnerVisitor();
    private final SymbolPrinter symbolPrinter;
    private final TransportCreateIngestRuleAction transportCreateIngestRuleAction;
    private final TransportDropIngestRuleAction transportDropIngestRuleAction;
    private final UserManager userManager;

    @Inject
    public DCLStatementDispatcher(SymbolPrinter symbolPrinter,
                                  Provider<UserManager> userManagerProvider,
                                  Provider<TransportCreateIngestRuleAction> transportCreateIngestRuleActionProvider,
                                  Provider<TransportDropIngestRuleAction> transportDropIngestRuleActionProvider) {
        this.symbolPrinter = symbolPrinter;
        this.userManager = userManagerProvider.get();
        this.transportCreateIngestRuleAction = transportCreateIngestRuleActionProvider.get();
        this.transportDropIngestRuleAction = transportDropIngestRuleActionProvider.get();
    }

    @Override
    public CompletableFuture<Long> apply(AnalyzedStatement analyzedStatement, Row row) {
        return INNER_VISITOR.process(analyzedStatement, userManager);
    }

    private class InnerVisitor extends AnalyzedStatementVisitor<UserManager, CompletableFuture<Long>> {

        @Override
        protected CompletableFuture<Long> visitAnalyzedStatement(AnalyzedStatement analyzedStatement, UserManager userManager) {
            return CompletableFutures.failedFuture(
                new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle \"%s\"", analyzedStatement)));
        }

        @Override
        public CompletableFuture<Long> visitPrivilegesStatement(PrivilegesAnalyzedStatement analysis, UserManager userManager) {
            return userManager.applyPrivileges(analysis.userNames(), analysis.privileges());
        }

        @Override
        public CompletableFuture<Long> visitCreateIngestRuleStatement(CreateIngestionRuleAnalysedStatement analysis, UserManager context) {
            IngestRule ingestRule =
                new IngestRule(analysis.ruleName(), analysis.targetTable().fqn(), getFormattedCondition(analysis));
            FutureActionListener<IngestRuleResponse, Long> listener =
                new FutureActionListener<>(r -> 1L);
            transportCreateIngestRuleAction.execute(
                new CreateIngestRuleRequest(analysis.sourceName(), ingestRule), listener);
            return listener;
        }

        private String getFormattedCondition(CreateIngestionRuleAnalysedStatement analysis) {
            return analysis.whereClause() != null ? ExpressionFormatter.formatStandaloneExpression(analysis.whereClause(),
                    new ExpressionParameterSubstitutionFormatter(symbolPrinter, analysis.parameterContext())) :
                    "";
        }

        @Override
        public CompletableFuture<Long> visitDropIngestRuleStatement(DropIngestionRuleAnalysedStatement analysis, UserManager context) {
            FutureActionListener<IngestRuleResponse, Long> listener =
                new FutureActionListener<IngestRuleResponse, Long>(r -> 1L) {
                    @Override
                    public void onFailure(Exception e) {
                        if (analysis.ifExists() &&
                            SQLExceptions.unwrap(e) instanceof ResourceNotFoundException) {
                            complete(0L);
                        } else {
                            super.onFailure(e);
                        }
                    }
                };
            transportDropIngestRuleAction.execute(new DropIngestRuleRequest(analysis.ruleName()), listener);
            return listener;
        }
    }
}

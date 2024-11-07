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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.collections.Tuple;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.sql.tree.AnalyzerElement;
import io.crate.sql.tree.CharFilters;
import io.crate.sql.tree.CreateAnalyzer;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.NamedProperties;
import io.crate.sql.tree.TokenFilters;
import io.crate.sql.tree.Tokenizer;

class CreateAnalyzerStatementAnalyzer {

    private final FulltextAnalyzerResolver ftResolver;
    private final NodeContext nodeCtx;

    CreateAnalyzerStatementAnalyzer(FulltextAnalyzerResolver ftResolver,
                                    NodeContext nodeCtx) {
        this.ftResolver = ftResolver;
        this.nodeCtx = nodeCtx;
    }

    private static class Context {

        @Nullable
        private Tuple<String, GenericProperties<Symbol>> tokenizer;
        private final Map<String, Symbol> genericAnalyzerProperties;
        private final Map<String, GenericProperties<Symbol>> charFilters;
        private final Map<String, GenericProperties<Symbol>> tokenFilters;

        private final ExpressionAnalyzer exprAnalyzerWithFieldsAsString;
        private final ExpressionAnalysisContext exprContext;

        Context(CoordinatorTxnCtx transactionContext,
                NodeContext nodeCtx,
                ParamTypeHints paramTypeHints) {
            this.genericAnalyzerProperties = new HashMap<>();
            this.charFilters = new HashMap<>();
            this.tokenFilters = new HashMap<>();

            this.exprContext = new ExpressionAnalysisContext(transactionContext.sessionSettings());
            this.exprAnalyzerWithFieldsAsString = new ExpressionAnalyzer(
                transactionContext,
                nodeCtx,
                paramTypeHints,
                FieldProvider.TO_LITERAL_VALIDATE_NAME,
                null);
        }
    }

    public AnalyzedStatement analyze(CreateAnalyzer<Expression> createAnalyzer,
                                     ParamTypeHints paramTypeHints,
                                     CoordinatorTxnCtx transactionContext) {
        String analyzerIdent = createAnalyzer.ident();
        if (analyzerIdent.equalsIgnoreCase("default")) {
            throw new IllegalArgumentException("Overriding the default analyzer is forbidden");
        }
        if (ftResolver.hasBuiltInAnalyzer(analyzerIdent)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH, "Cannot override builtin analyzer '%s'", analyzerIdent));
        }

        String extendedAnalyzerName;
        if (createAnalyzer.isExtending()) {
            extendedAnalyzerName = createAnalyzer.extendedAnalyzer();
            if (!ftResolver.hasAnalyzer(extendedAnalyzerName)) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH, "Extended Analyzer '%s' does not exist", extendedAnalyzerName));
            }
        } else {
            extendedAnalyzerName = null;
        }

        var context = new Context(
            transactionContext,
            nodeCtx,
            paramTypeHints
        );

        for (AnalyzerElement<Expression> element : createAnalyzer.elements()) {
            AnalyzerElementsAnalysisVisitor.analyze(element, context);
        }

        return new AnalyzedCreateAnalyzer(
            analyzerIdent,
            extendedAnalyzerName,
            context.tokenizer,
            new GenericProperties<>(context.genericAnalyzerProperties),
            context.tokenFilters,
            context.charFilters);
    }

    private static class AnalyzerElementsAnalysisVisitor
        extends DefaultTraversalVisitor<Void, CreateAnalyzerStatementAnalyzer.Context> {

        static final AnalyzerElementsAnalysisVisitor INSTANCE = new AnalyzerElementsAnalysisVisitor();

        static Void analyze(AnalyzerElement<Expression> node, Context context) {
            node.accept(INSTANCE, context);
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Void visitTokenizer(Tokenizer<?> node, Context context) {
            var tokenizer = (Tokenizer<Expression>) node;

            GenericProperties<Symbol> properties = tokenizer.properties()
                .map(p -> context.exprAnalyzerWithFieldsAsString.convert(p, context.exprContext));

            context.tokenizer = new Tuple<>(tokenizer.ident(), properties);
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Void visitGenericProperty(GenericProperty<?> node, Context context) {
            var property = (GenericProperty<Expression>) node;

            context.genericAnalyzerProperties.put(
                property.key(),
                context.exprAnalyzerWithFieldsAsString.convert(
                    property.value(),
                    context.exprContext)
            );
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Void visitTokenFilters(TokenFilters<?> node, Context context) {
            var tokenFilters = (TokenFilters<Expression>) node;

            for (NamedProperties<Expression> tokenFilter : tokenFilters.tokenFilters()) {
                GenericProperties<Symbol> properties = tokenFilter.properties()
                    .map(p -> context.exprAnalyzerWithFieldsAsString.convert(p, context.exprContext));

                context.tokenFilters.put(tokenFilter.ident(), properties);
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Void visitCharFilters(CharFilters<?> node, Context context) {
            var charFilters = (CharFilters<Expression>) node;

            for (NamedProperties<Expression> charFilter : charFilters.charFilters()) {
                GenericProperties<Symbol> properties = charFilter.properties()
                    .map(p -> context.exprAnalyzerWithFieldsAsString.convert(p, context.exprContext));

                context.charFilters.put(charFilter.ident(), properties);
            }
            return null;
        }
    }
}

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

package io.crate.analyze.relations.select;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.OutputNameFormatter;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.validator.SelectSymbolValidator;
import io.crate.metadata.OutputName;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectAnalyzer {

    private static final InnerVisitor INSTANCE = new InnerVisitor();

    public static SelectAnalysis analyzeSelect(Select select,
                                               Map<QualifiedName, AnalyzedRelation> sources,
                                               ExpressionAnalyzer expressionAnalyzer,
                                               ExpressionAnalysisContext expressionAnalysisContext) {
        SelectAnalysis selectAnalysis = new SelectAnalysis(sources, expressionAnalyzer, expressionAnalysisContext);
        INSTANCE.process(select, selectAnalysis);
        SelectSymbolValidator.validate(selectAnalysis.outputSymbols);
        return selectAnalysis;
    }

    public static class SelectAnalysis {

        private Map<QualifiedName, AnalyzedRelation> sources;
        private ExpressionAnalyzer expressionAnalyzer;
        private ExpressionAnalysisContext expressionAnalysisContext;
        private List<OutputName> outputNames = new ArrayList<>();
        private List<Symbol> outputSymbols = new ArrayList<>();
        private Multimap<String, Symbol> outputMultiMap = HashMultimap.create();

        private SelectAnalysis(Map<QualifiedName, AnalyzedRelation> sources,
                               ExpressionAnalyzer expressionAnalyzer,
                               ExpressionAnalysisContext expressionAnalysisContext) {
            this.sources = sources;
            this.expressionAnalyzer = expressionAnalyzer;
            this.expressionAnalysisContext = expressionAnalysisContext;
        }

        public List<OutputName> outputNames() {
            return outputNames;
        }

        public List<Symbol> outputSymbols() {
            return outputSymbols;
        }

        Symbol toSymbol(Expression expression) {
            return expressionAnalyzer.convert(expression, expressionAnalysisContext);
        }

        /**
         * multiMap containing outputNames() as keys and outputSymbols() as values.
         * Can be used to resolve expressions in ORDER BY or GROUP BY where it is important to know
         * if a outputName is unique
         */
        public Multimap<String, Symbol> outputMultiMap() {
            return outputMultiMap;
        }

        void add(String outputName, Symbol symbol) {
            outputNames.add(new OutputName(outputName));
            outputSymbols.add(symbol);
            outputMultiMap.put(outputName, symbol);
        }
    }

    private static class InnerVisitor extends DefaultTraversalVisitor<Void, SelectAnalysis> {

        @Override
        protected Void visitSingleColumn(SingleColumn node, SelectAnalysis context) {
            Symbol symbol = context.toSymbol(node.getExpression());
            if (node.getAlias().isPresent()) {
                context.add(node.getAlias().get(), symbol);
            } else {
                context.add(OutputNameFormatter.format(node.getExpression()), symbol);
            }
            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, SelectAnalysis context) {
            for (AnalyzedRelation relation : context.sources.values()) {
                for (Field field : relation.fields()) {
                    context.add(field.path().outputName(), field);
                }
            }
            return null;
        }
    }
}

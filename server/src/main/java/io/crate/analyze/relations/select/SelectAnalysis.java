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

package io.crate.analyze.relations.select;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectAnalysis {

    private final Map<RelationName, AnalyzedRelation> sources;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionAnalysisContext;
    private final List<Symbol> outputSymbols;
    private final Multimap<String, Symbol> outputMultiMap;

    public SelectAnalysis(int expectedItems,
                          Map<RelationName, AnalyzedRelation> sources,
                          ExpressionAnalyzer expressionAnalyzer,
                          ExpressionAnalysisContext expressionAnalysisContext) {
        this.sources = sources;
        this.expressionAnalyzer = expressionAnalyzer;
        this.expressionAnalysisContext = expressionAnalysisContext;
        outputMultiMap = HashMultimap.create(expectedItems, 1);
        outputSymbols = new ArrayList<>(expectedItems);
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    Symbol toSymbol(Expression expression) {
        return expressionAnalyzer.convert(expression, expressionAnalysisContext);
    }

    public Map<RelationName, AnalyzedRelation> sources() {
        return sources;
    }

    /**
     * multiMap containing outputNames() as keys and outputSymbols() as values.
     * Can be used to resolve expressions in ORDER BY or GROUP BY where it is important to know
     * if a outputName is unique
     */
    public Multimap<String, Symbol> outputMultiMap() {
        return outputMultiMap;
    }

    public void add(ColumnIdent path, Symbol symbol) {
        outputSymbols.add(symbol);
        outputMultiMap.put(path.sqlFqn(), symbol);
    }
}

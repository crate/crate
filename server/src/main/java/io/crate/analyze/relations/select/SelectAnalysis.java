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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.Expression;

public class SelectAnalysis {

    private final Map<RelationName, AnalyzedRelation> sources;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionAnalysisContext;
    private final List<Symbol> outputSymbols;
    private final List<String> outputNames;
    private final Map<String, Set<Symbol>> outputMap;

    public SelectAnalysis(int expectedItems,
                          Map<RelationName, AnalyzedRelation> sources,
                          ExpressionAnalyzer expressionAnalyzer,
                          ExpressionAnalysisContext expressionAnalysisContext) {
        this.sources = sources;
        this.expressionAnalyzer = expressionAnalyzer;
        this.expressionAnalysisContext = expressionAnalysisContext;
        outputMap = new HashMap<>();
        outputSymbols = new ArrayList<>(expectedItems);
        outputNames = new ArrayList<>(expectedItems);
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
    public Map<String, Set<Symbol>> outputMultiMap() {
        return outputMap;
    }

    public List<String> outputColumnNames() {
        return outputNames;
    }

    public void add(ColumnIdent path, Symbol symbol, @Nullable String name) {
        outputSymbols.add(symbol);
        outputNames.add(name == null ? path.sqlFqn() : name);
        var symbols = outputMap.get(path.sqlFqn());
        if (symbols == null) {
            symbols = new HashSet<>();
        }
        symbols.add(symbol);
        outputMap.put(path.sqlFqn(), symbols);
    }
}

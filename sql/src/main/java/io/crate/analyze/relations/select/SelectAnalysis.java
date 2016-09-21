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

package io.crate.analyze.relations.select;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalysisContext;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectAnalysis {

    private final Map<QualifiedName, AnalyzedRelation> sources;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final ExpressionAnalysisContext expressionAnalysisContext;
    private final List<Path> outputNames = new ArrayList<>();
    private final List<Symbol> outputSymbols = new ArrayList<>();
    private final Multimap<String, Symbol> outputMultiMap = HashMultimap.create();

    public SelectAnalysis(RelationAnalysisContext context,
                          ExpressionAnalyzer expressionAnalyzer,
                          ExpressionAnalysisContext expressionAnalysisContext) {
        this.sources = context.sources();
        this.expressionAnalyzer = expressionAnalyzer;
        this.expressionAnalysisContext = expressionAnalysisContext;
    }

    public List<Path> outputNames() {
        return outputNames;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    Symbol toSymbol(Expression expression) {
        return expressionAnalyzer.convert(expression, expressionAnalysisContext);
    }

    public Map<QualifiedName, AnalyzedRelation> sources() {
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

    public void add(Path path, Symbol symbol) {
        outputNames.add(path);
        outputSymbols.add(symbol);
        outputMultiMap.put(path.outputName(), symbol);
    }
}

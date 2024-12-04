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

import java.util.Map;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Tuple;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.GenericProperties;

public class AnalyzedCreateAnalyzer implements DDLStatement {

    private final String ident;
    @Nullable
    private final String extendedAnalyzerName;
    @Nullable
    private final Tuple<String, GenericProperties<Symbol>> tokenizer;
    private final GenericProperties<Symbol> genericAnalyzerProperties;
    private final Map<String, GenericProperties<Symbol>> charFilters;
    private final Map<String, GenericProperties<Symbol>> tokenFilters;

    AnalyzedCreateAnalyzer(String ident,
                           @Nullable String extendedAnalyzerName,
                           @Nullable Tuple<String, GenericProperties<Symbol>> tokenizer,
                           GenericProperties<Symbol> genericAnalyzerProperties,
                           Map<String, GenericProperties<Symbol>> tokenFilters,
                           Map<String, GenericProperties<Symbol>> charFilters) {
        this.ident = ident;
        this.extendedAnalyzerName = extendedAnalyzerName;
        this.tokenizer = tokenizer;
        this.genericAnalyzerProperties = genericAnalyzerProperties;
        this.tokenFilters = tokenFilters;
        this.charFilters = charFilters;
    }

    public String ident() {
        return ident;
    }

    @Nullable
    public String extendedAnalyzerName() {
        return extendedAnalyzerName;
    }

    @Nullable
    public Tuple<String, GenericProperties<Symbol>> tokenizer() {
        return tokenizer;
    }

    public Map<String, GenericProperties<Symbol>> charFilters() {
        return charFilters;
    }

    public Map<String, GenericProperties<Symbol>> tokenFilters() {
        return tokenFilters;
    }

    public GenericProperties<Symbol> genericAnalyzerProperties() {
        return genericAnalyzerProperties;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitCreateAnalyzerStatement(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        if (tokenizer != null) {
            tokenizer.v2().forValues(consumer);
        }
        tokenFilters.values().forEach(x -> x.forValues(consumer));
        charFilters.values().forEach(x -> x.forValues(consumer));
        genericAnalyzerProperties.forValues(consumer);
    }
}

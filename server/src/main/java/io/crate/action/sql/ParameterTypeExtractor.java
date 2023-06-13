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

package io.crate.action.sql;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.Relations;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;

import org.jetbrains.annotations.NotNull;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

class ParameterTypeExtractor extends DefaultTraversalSymbolVisitor<Void, Void> implements Consumer<Symbol> {

    private final SortedSet<ParameterSymbol> parameterSymbols;

    ParameterTypeExtractor() {
        this.parameterSymbols = new TreeSet<>(Comparator.comparing(ParameterSymbol::index));
    }

    @Override
    public void accept(Symbol symbol) {
        symbol.accept(this, null);
    }

    @Override
    public Void visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
        Relations.traverseDeepSymbols(selectSymbol.relation(), this);
        return null;
    }

    @Override
    public Void visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
        parameterSymbols.add(parameterSymbol);
        return null;
    }

    /**
     * Gets the parameters from the AnalyzedStatement, if possible.
     * @param consumer A consumer which takes a symbolVisitor;
     *                 This symbolVisitor should visit all {@link ParameterSymbol}s in a {@link AnalyzedStatement}
     * @return A sorted array with the parameters ($1 comes first, then $2, etc.) or null if
     *         parameters can't be obtained.
     */
    DataType<?>[] getParameterTypes(@NotNull Consumer<Consumer<? super Symbol>> consumer) {
        consumer.accept(this);
        if (!parameterSymbols.isEmpty() && parameterSymbols.last().index() != parameterSymbols.size() - 1) {
            throw new IllegalStateException("The assembled list of ParameterSymbols is invalid. Missing parameters.");
        }
        DataType<?>[] dataTypes = parameterSymbols.stream()
            .map(ParameterSymbol::getBoundType)
            .toArray(DataType[]::new);
        parameterSymbols.clear();
        return dataTypes;
    }
}

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

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.collections.Lists;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;

public class ParameterTypes extends DefaultTraversalSymbolVisitor<Set<ParameterSymbol>, Void> {

    private static final ParameterTypes INSTANCE = new ParameterTypes();

    private ParameterTypes() {
    }

    @VisibleForTesting
    static List<DataType<?>> extract(Consumer<Consumer<? super Symbol>> consumer) {
        TreeSet<ParameterSymbol> parameterSymbols = new TreeSet<>(Comparator.comparing(ParameterSymbol::index));
        consumer.accept(symbol -> symbol.accept(INSTANCE, parameterSymbols));
        if (!parameterSymbols.isEmpty() && parameterSymbols.last().index() != parameterSymbols.size() - 1) {
            throw new IllegalStateException("The assembled list of ParameterSymbols is invalid. Missing parameters.");
        }
        return Lists.map(parameterSymbols, ParameterSymbol::getBoundType);
    }

    public static List<DataType<?>> extract(AnalyzedStatement statement) {
        return extract(consumer -> Relations.traverseDeepSymbols(statement, consumer));
    }

    @Override
    public Void visitSelectSymbol(SelectSymbol selectSymbol, Set<ParameterSymbol> parameterSymbols) {
        Relations.traverseDeepSymbols(selectSymbol.relation(), symbol -> symbol.accept(this, parameterSymbols));
        return null;
    }

    @Override
    public Void visitParameterSymbol(ParameterSymbol parameterSymbol, Set<ParameterSymbol> parameterSymbols) {
        parameterSymbols.add(parameterSymbol);
        return null;
    }
}

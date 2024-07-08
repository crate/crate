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

package io.crate.analyze.validator;

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Reference;


public class HavingSymbolValidator {

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol, @Nullable List<Symbol> groupBySymbols) throws IllegalArgumentException {
        symbol.accept(INNER_VALIDATOR, new HavingContext(groupBySymbols));
    }

    static class HavingContext {

        @Nullable
        private final List<Symbol> groupBySymbols;

        private boolean insideAggregation = false;

        HavingContext(@Nullable List<Symbol> groupBySymbols) {
            this.groupBySymbols = groupBySymbols;
        }

        boolean groupByContains(Symbol symbol) {
            return groupBySymbols != null && groupBySymbols.contains(symbol);
        }
    }

    private static class InnerValidator extends SymbolVisitor<HavingContext, Void> {

        private static void ensurePresentInGroupBY(Symbol symbol, HavingContext context) {
            if (!context.insideAggregation && !context.groupByContains(symbol)) {
                throw new IllegalArgumentException(
                        Symbols.format("Cannot use column %s outside of an Aggregation in HAVING clause. " +
                                       "Only GROUP BY keys allowed here.", symbol));
            }
        }

        @Override
        public Void visitField(ScopedSymbol field, HavingContext context) {
            ensurePresentInGroupBY(field, context);
            return null;
        }

        @Override
        public Void visitReference(Reference ref, HavingContext context) {
            ensurePresentInGroupBY(ref, context);
            return null;
        }

        @Override
        public Void visitFunction(Function function, HavingContext context) {
            FunctionType type = function.signature().getType();
            if (type == FunctionType.TABLE) {
                throw new IllegalArgumentException("Table functions are not allowed in HAVING");
            } else if (type == FunctionType.AGGREGATE) {
                context.insideAggregation = true;
            } else {
                // allow function if it is part of the grouping symbols
                if (context.groupByContains(function)) {
                    return null;
                }
            }

            for (Symbol argument : function.arguments()) {
                argument.accept(this, context);
            }
            if (type == FunctionType.AGGREGATE) {
                context.insideAggregation = false;
            }
            return null;
        }

        @Override
        public Void visitWindowFunction(WindowFunction symbol, HavingContext context) {
            throw new IllegalArgumentException("Window functions are not allowed in HAVING");
        }

        @Override
        protected Void visitSymbol(Symbol symbol, HavingContext context) {
            return null;
        }
    }
}

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

package io.crate.planner.consumer;

import java.util.Collection;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;

/**
 * Sort symbol validation when aggregation is also used.
 * <p>
 * Check if the SORT BY field is in the SELECT statement when aggregation is used.
 * Scalar functions can though be used on top of the field.
 */
public class OrderByWithAggregationValidator {

    private static final String INVALID_FIELD_TEMPLATE = "ORDER BY expression '%s' must appear in the select clause " +
                                                         "when grouping or global aggregation is used";
    private static final String INVALID_FIELD_IN_DISTINCT_TEMPLATE = "ORDER BY expression '%s' must appear in the " +
                                                                     "select clause when SELECT DISTINCT is used";

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol,
                                Collection<? extends Symbol> outputSymbols,
                                boolean isDistinct) throws UnsupportedOperationException {
        symbol.accept(INNER_VALIDATOR, new ValidatorContext(outputSymbols, isDistinct));
    }

    private static class ValidatorContext {
        private final Collection<? extends Symbol> outputSymbols;
        private final boolean isDistinct;
        @Nullable
        private Function aggregateFunctionVisited;

        ValidatorContext(Collection<? extends Symbol> outputSymbols, boolean isDistinct) {
            this.outputSymbols = outputSymbols;
            this.isDistinct = isDistinct;
        }
    }

    private static class InnerValidator extends SymbolVisitor<ValidatorContext, Void> {

        @Override
        public Void visitFunction(Function symbol, ValidatorContext context) {
            for (var output : context.outputSymbols) {
                if (output.equals(symbol)) {
                    return null;
                } else if (output instanceof AliasSymbol && ((AliasSymbol) output).symbol().equals(symbol)) {
                    return null;
                }
            }
            if (context.isDistinct) {
                throw new UnsupportedOperationException(Symbols.format(INVALID_FIELD_IN_DISTINCT_TEMPLATE, symbol));
            }
            switch (symbol.signature().getType()) {
                case AGGREGATE:
                    if (context.aggregateFunctionVisited == null) {
                        context.aggregateFunctionVisited = symbol;
                        for (Symbol arg : symbol.arguments()) {
                            arg.accept(this, context);
                        }
                        break;
                    } else {
                        throw new UnsupportedOperationException(
                            Symbols.format("Aggregate function calls cannot be nested '%s'.", context.aggregateFunctionVisited)
                        );
                    }
                case SCALAR:
                    for (Symbol arg : symbol.arguments()) {
                        arg.accept(this, context);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                        Symbols.format("ORDER BY function '%s' is not allowed. " +
                                       "Only scalar or aggregate functions can be used", symbol));
            }
            return null;
        }

        @Override
        public Void visitReference(Reference ref, ValidatorContext context) {
            if (context.aggregateFunctionVisited == null) {
                return ensureOutputsContainColumn(ref, context);
            } else {
                // References which are not part of the output columns are allowed when inside
                // an aggregation function like `select x from tbl group by x order by count(y)`
                return null;
            }
        }

        @Override
        public Void visitField(ScopedSymbol field, ValidatorContext context) {
            return ensureOutputsContainColumn(field, context);
        }

        private static Void ensureOutputsContainColumn(Symbol symbol, ValidatorContext context) {
            if (context.outputSymbols.contains(symbol)) {
                return null;
            } else {
                for (Symbol outputSymbol : context.outputSymbols) {
                    if (outputSymbol instanceof AliasSymbol) {
                        if (((AliasSymbol) outputSymbol).symbol().equals(symbol)) {
                            return null;
                        }
                    }
                }
                String template = context.isDistinct ? INVALID_FIELD_IN_DISTINCT_TEMPLATE : INVALID_FIELD_TEMPLATE;
                throw new UnsupportedOperationException(Symbols.format(template, symbol));
            }
        }

        @Override
        public Void visitSymbol(Symbol symbol, ValidatorContext context) {
            return null;
        }
    }
}

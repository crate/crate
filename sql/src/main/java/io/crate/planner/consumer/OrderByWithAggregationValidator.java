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

package io.crate.planner.consumer;

import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.FunctionInfo;

import java.util.Collection;

/**
 * Sort symbol validation when aggregation is also used.
 * <p>
 * Check if the SORT BY field is in the SELECT statement when aggregation is used.
 * Scalar functions can though be used on top of the field.
 */
public class OrderByWithAggregationValidator {

    private static final  String INVALID_FIELD_TEMPLATE = "ORDER BY expression '%s' must appear in the select clause " +
                                                         "when grouping or global aggregation is used";
    private static final  String INVALID_FIELD_IN_DISTINCT_TEMPLATE = "ORDER BY expression '%s' must appear in the " +
                                                                     "select clause when SELECT DISTINCT is used";

    private static final  InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol,
                                Collection<? extends Symbol> outputSymbols,
                                boolean isDistinct) throws UnsupportedOperationException {
        INNER_VALIDATOR.process(symbol, new ValidatorContext(outputSymbols, isDistinct));
    }

    private static class ValidatorContext {
        private final Collection<? extends Symbol> outputSymbols;
        private final boolean isDistinct;

        ValidatorContext(Collection<? extends Symbol> outputSymbols, boolean isDistinct) {
            this.outputSymbols = outputSymbols;
            this.isDistinct = isDistinct;
        }
    }

    private static class InnerValidator extends SymbolVisitor<ValidatorContext, Void> {

        @Override
        public Void visitFunction(Function symbol, ValidatorContext context) {
            if (context.outputSymbols.contains(symbol)) {
                return null;
            } else if (context.isDistinct) {
                throw new UnsupportedOperationException(SymbolFormatter.format(INVALID_FIELD_IN_DISTINCT_TEMPLATE, symbol));
            }
            if (symbol.info().type() == FunctionInfo.Type.SCALAR) {
                for (Symbol arg : symbol.arguments()) {
                    process(arg, context);
                }
            } else {
                throw new UnsupportedOperationException(
                    SymbolFormatter.format("ORDER BY function '%s' is not allowed. " +
                                           "Only scalar functions can be used", symbol));
            }
            return null;
        }

        @Override
        public Void visitField(Field field, ValidatorContext context) {
            if (context.outputSymbols.contains(field)) {
                return null;
            } else {
                String template = context.isDistinct ? INVALID_FIELD_IN_DISTINCT_TEMPLATE : INVALID_FIELD_TEMPLATE;
                throw new UnsupportedOperationException(SymbolFormatter.format(template, field));
            }
        }

        @Override
        public Void visitSymbol(Symbol symbol, ValidatorContext context) {
            return null;
        }
    }
}

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

package io.crate.analyze.validator;

import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.types.DataTypes;

import java.util.Locale;

/**
 * rudimentary sort symbol validation that can be used during analysis.
 * <p>
 * validates only types and that there are no predicates.
 */
public class SemanticSortValidator {

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol) throws UnsupportedOperationException {
        INNER_VALIDATOR.process(symbol, new SortContext());
    }

    static class SortContext {
        private boolean inFunction;

        SortContext() {
            this.inFunction = false;
        }
    }

    private static class InnerValidator extends SymbolVisitor<SortContext, Void> {

        @Override
        public Void visitFunction(Function symbol, SortContext context) {
            if (!context.inFunction && !DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH,
                        "Cannot ORDER BY '%s': invalid return type '%s'.",
                        SymbolPrinter.INSTANCE.printUnqualified(symbol),
                        symbol.valueType())
                );
            }
            try {
                context.inFunction = true;
                for (Symbol arg : symbol.arguments()) {
                    process(arg, context);
                }
            } finally {
                context.inFunction = false;
            }
            return null;
        }

        public Void visitMatchPredicate(MatchPredicate matchPredicate, SortContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "%s predicate cannot be used in an ORDER BY clause", io.crate.expression.predicate.MatchPredicate.NAME));
        }

        @Override
        public Void visitField(Field field, SortContext context) {
            // if we are in a function, we do not need to check the data type.
            // the function will do that for us.
            if (!context.inFunction && !DataTypes.PRIMITIVE_TYPES.contains(field.valueType())) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH,
                        "Cannot ORDER BY '%s': invalid data type '%s'.",
                        SymbolPrinter.INSTANCE.printUnqualified(field),
                        field.valueType())
                );
            }
            return null;
        }

        @Override
        public Void visitSymbol(Symbol symbol, SortContext context) {
            return null;
        }
    }
}

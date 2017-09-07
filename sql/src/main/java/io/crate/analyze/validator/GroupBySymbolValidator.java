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

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.MatchPredicate;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Locale;

public class GroupBySymbolValidator {

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol) throws IllegalArgumentException, UnsupportedOperationException {
        INNER_VALIDATOR.process(symbol, null);
    }

    private static void validateDataType(Symbol symbol) {
        if (!DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Cannot GROUP BY '%s': invalid data type '%s'",
                    SymbolPrinter.INSTANCE.printSimple(symbol),
                    symbol.valueType()));
        }
    }

    private static class InnerValidator extends SymbolVisitor<Function, Void> {

        @Override
        public Void visitFunction(Function function, @Nullable Function parentScalar) {
            switch (function.info().type()) {
                case SCALAR:
                    // If is literal no further datatype validation is required
                    if (Symbols.allLiterals(function)) {
                        return null;
                    }

                    visitSymbol(function, parentScalar);
                    if (parentScalar == null) {
                        parentScalar = function;
                    }
                    for (Symbol argument : function.arguments()) {
                        process(argument, parentScalar);
                    }
                    break;
                case AGGREGATE:
                    throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
                default:
                    throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH, "FunctionInfo.Type %s not handled", function.info().type()));
            }
            return null;
        }

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, @Nullable Function parentScalar) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "%s predicate cannot be used in a GROUP BY clause", io.crate.operation.predicate.MatchPredicate.NAME));
        }

        @Override
        protected Void visitSymbol(Symbol symbol, @Nullable Function parentScalar) {
            // If is literal no further datatype validation is required
            if (Symbols.allLiterals(symbol)) {
                return null;
            }

            if (parentScalar == null) {
                validateDataType(symbol);
            } else {
                validateDataType(parentScalar);
            }
            return null;
        }
    }

}

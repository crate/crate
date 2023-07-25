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

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.types.BitStringType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;

/**
 * rudimentary sort symbol validation that can be used during analysis.
 * <p>
 * validates only types and that there are no predicates.
 */
public class SemanticSortValidator {

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    // Instead of this we should probably have a property on DataType
    // that indicates if ordering is supported and encapsulate the required functionality somehow
    public static final Set<Integer> SUPPORTED_TYPES = Stream.concat(
        DataTypes.PRIMITIVE_TYPES.stream(),
        Stream.of(
            DataTypes.REGCLASS,
            DataTypes.REGPROC,
            BitStringType.INSTANCE_ONE,
            FloatVectorType.INSTANCE_ONE
        )
    ).map(x -> x.id()).collect(Collectors.toSet());

    public static void validate(Symbol symbol) throws UnsupportedOperationException {
        symbol.accept(INNER_VALIDATOR, new SortContext("ORDER BY"));
    }

    /**
     * @param symbol
     * @param operation there are operations other than the `ORDER BY` that will translate into a sorting operation.
     *                  this represents the name of these possible operations and will be used to customise the error
     *                  reporting message.
     * @throws UnsupportedOperationException
     */
    public static void validate(Symbol symbol, String operation) throws UnsupportedOperationException {
        symbol.accept(INNER_VALIDATOR, new SortContext(operation));
    }

    static class SortContext {
        private final String operation;
        private boolean inFunction;

        SortContext(String operation) {
            this.operation = operation;
            this.inFunction = false;
        }
    }

    private static class InnerValidator extends SymbolVisitor<SortContext, Void> {

        @Override
        public Void visitFunction(Function symbol, SortContext context) {
            if (!context.inFunction && !SUPPORTED_TYPES.contains(symbol.valueType().id())) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH,
                                  "Cannot %s '%s': invalid return type '%s'.",
                                  context.operation,
                                  symbol,
                                  symbol.valueType())
                );
            }
            try {
                context.inFunction = true;
                for (Symbol arg : symbol.arguments()) {
                    arg.accept(this, context);
                }
            } finally {
                context.inFunction = false;
            }
            return null;
        }

        public Void visitMatchPredicate(MatchPredicate matchPredicate, SortContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                                                                  "%s predicate cannot be used in an %s clause",
                                                                  io.crate.expression.predicate.MatchPredicate.NAME,
                                                                  context.operation));
        }

        @Override
        public Void visitSymbol(Symbol symbol, SortContext context) {
            // if we are in a function, we do not need to check the data type.
            // the function will do that for us.
            if (!context.inFunction && !SUPPORTED_TYPES.contains(symbol.valueType().id())) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH,
                                  "Cannot %s '%s': invalid data type '%s'.",
                                  context.operation,
                                  symbol,
                                  symbol.valueType())
                );
            }
            return null;
        }
    }
}

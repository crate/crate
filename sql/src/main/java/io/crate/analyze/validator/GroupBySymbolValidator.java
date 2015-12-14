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

import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.types.DataTypes;

public class GroupBySymbolValidator {

    private final static InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol) throws IllegalArgumentException, UnsupportedOperationException {
        INNER_VALIDATOR.process(symbol, new Context());
    }

    private static class Context {
        private boolean insideFunction = false;
    }

    private static class InnerValidator extends SymbolVisitor<Context, Void> {

        @Override
        public Void visitFunction(Function symbol, Context context) {
            switch (symbol.info().type()) {
                case SCALAR:
                    break;
                case AGGREGATE:
                    throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
                case PREDICATE:
                    throw new UnsupportedOperationException(String.format(
                            "%s predicate cannot be used in a GROUP BY clause", symbol.info().ident().name()));
                default:
                    throw new UnsupportedOperationException(
                            String.format("FunctionInfo.Type %s not handled", symbol.info().type()));
            }
            context.insideFunction = true;
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            context.insideFunction = false;
            return null;
        }

        @Override
        public Void visitField(Field field, Context context) {
            // if insideFunction the type here doesn't matter, only the returnType of the function itself will matter.
            if (!context.insideFunction && !DataTypes.PRIMITIVE_TYPES.contains(field.valueType())) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': invalid data type '%s'",
                                SymbolPrinter.INSTANCE.printSimple(field),
                                field.valueType()));
            }
            return null;
        }

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, Context context) {
            throw new UnsupportedOperationException(String.format(
                    "%s predicate cannot be used in a GROUP BY clause", io.crate.operation.predicate.MatchPredicate.NAME));
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            return null;
        }
    }
}
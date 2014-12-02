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

import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;

public class GroupBySymbolValidator {

    private final static InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol) throws IllegalArgumentException, UnsupportedOperationException {
        INNER_VALIDATOR.process(symbol, null);
    }

    private static class InnerValidator extends SymbolVisitor<Void, Void> {

        @Override
        public Void visitDynamicReference(DynamicReference symbol, Void context) {
            throw new IllegalArgumentException(
                    SymbolFormatter.format("unknown column '%s' not allowed in GROUP BY", symbol));
        }

        @Override
        public Void visitReference(Reference symbol, Void context) {
            if (!DataTypes.PRIMITIVE_TYPES.contains(symbol.valueType())) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': invalid data type '%s'",
                                SymbolFormatter.format(symbol),
                                symbol.valueType()));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': grouping on analyzed/fulltext columns is not possible",
                                SymbolFormatter.format(symbol)));
            } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                throw new IllegalArgumentException(
                        String.format("Cannot GROUP BY '%s': grouping on non-indexed columns is not possible",
                                SymbolFormatter.format(symbol)));
            }
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, Void context) {
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
            return null;
        }

        @Override
        public Void visitField(Field field, Void context) {
            return process(field.target(), context);
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Void context) {
            throw new UnsupportedOperationException(
                    String.format("Cannot GROUP BY for '%s'", SymbolFormatter.format(symbol))
            );
        }
    }
}
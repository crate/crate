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

import java.util.Collection;

public class SelectSymbolValidator {

    private final static InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Collection<Symbol> symbols, boolean selectFromFieldCache) {
        for (Symbol symbol : symbols) {
            INNER_VALIDATOR.process(symbol, new SelectContext(selectFromFieldCache));
        }
    }

    static class SelectContext {
        private boolean selectFromFieldCache;

        public SelectContext(boolean selectFromFieldCache) {
            this.selectFromFieldCache = selectFromFieldCache;
        }
    }

    private static class InnerValidator extends SymbolVisitor<SelectSymbolValidator.SelectContext, Void> {

        @Override
        public Void visitReference(Reference symbol, SelectContext context) {
            if (context.selectFromFieldCache) {
                if (symbol.info().indexType() == ReferenceInfo.IndexType.ANALYZED) {
                    throw new IllegalArgumentException(
                            String.format("Cannot select analyzed column '%s' " +
                                            "within grouping or aggregations",
                                    SymbolFormatter.format(symbol)));
                } else if (symbol.info().indexType() == ReferenceInfo.IndexType.NO) {
                    throw new IllegalArgumentException(
                            String.format("Cannot select non-indexed column '%s' " +
                                            "within grouping or aggregations",
                                    SymbolFormatter.format(symbol)));
                }
            }
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, SelectContext context) {
            switch (symbol.info().type()) {
                case SCALAR:
                    break;
                case AGGREGATE:
                    context.selectFromFieldCache = true;
                    break;
                case PREDICATE:
                    throw new UnsupportedOperationException(String.format(
                            "%s predicate cannot be selected", symbol.info().ident().name()));
                default:
                    throw new UnsupportedOperationException(String.format(
                            "FunctionInfo.Type %s not handled", symbol.info().type()));
            }
            for (Symbol arg : symbol.arguments()) {
                process(arg, context);
            }
            return null;
        }

        @Override
        public Void visitField(Field field, SelectContext context) {
            return process(field.target(), context);
        }

        @Override
        public Void visitSymbol(Symbol symbol, SelectContext context) {
            return null;
        }
    }
}
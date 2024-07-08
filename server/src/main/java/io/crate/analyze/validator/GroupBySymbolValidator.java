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

import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.WindowFunction;

public class GroupBySymbolValidator {

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol) throws IllegalArgumentException, UnsupportedOperationException {
        symbol.accept(INNER_VALIDATOR, false);
    }

    private static class InnerValidator extends SymbolVisitor<Boolean, Void> {

        @Override
        public Void visitFunction(Function function, Boolean insideScalar) {
            switch (function.signature().getType()) {
                case SCALAR:
                    for (Symbol argument : function.arguments()) {
                        argument.accept(this, true);
                    }
                    break;
                case AGGREGATE:
                    throw new IllegalArgumentException("Aggregate functions are not allowed in GROUP BY");
                case TABLE:
                    if (insideScalar == false) {
                        throw new IllegalArgumentException("Table functions are not allowed in GROUP BY");
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(
                        String.format(Locale.ENGLISH, "FunctionInfo.Type %s not handled", function.signature().getType()));
            }
            return null;
        }

        @Override
        public Void visitWindowFunction(WindowFunction symbol, Boolean insideScalar) {
            throw new IllegalArgumentException("Window functions are not allowed in GROUP BY");
        }

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, Boolean insideScalar) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "%s predicate cannot be used in a GROUP BY clause", io.crate.expression.predicate.MatchPredicate.NAME));
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Boolean insideScalar) {
            return null;
        }

        @Override
        public Void visitAlias(AliasSymbol aliasSymbol, Boolean insideScalar) {
            return aliasSymbol.symbol().accept(this, insideScalar);
        }
    }
}

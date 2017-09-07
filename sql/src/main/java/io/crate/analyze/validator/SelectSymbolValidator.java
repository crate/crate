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
import io.crate.exceptions.UnsupportedFeatureException;

import java.util.Collection;
import java.util.Locale;

public class SelectSymbolValidator {

    private static final InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Collection<Symbol> symbols) {
        for (Symbol symbol : symbols) {
            INNER_VALIDATOR.process(symbol, null);
        }
    }

    private static class InnerValidator extends SymbolVisitor<Void, Void> {

        @Override
        public Void visitFunction(Function symbol, Void context) {
            switch (symbol.info().type()) {
                case SCALAR:
                case AGGREGATE:
                    break;
                case TABLE:
                    throw new UnsupportedFeatureException("Table functions are not supported in select list");
                default:
                    throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                        "FunctionInfo.Type %s not handled", symbol.info().type()));
            }
            for (Symbol arg : symbol.arguments()) {
                process(arg, context);
            }
            return null;
        }

        @Override
        public Void visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
            throw new UnsupportedOperationException("match predicate cannot be selected");
        }

        @Override
        public Void visitSymbol(Symbol symbol, Void context) {
            return null;
        }
    }
}

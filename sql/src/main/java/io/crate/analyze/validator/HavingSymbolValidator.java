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

import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.List;

import static io.crate.planner.symbol.Field.unwrap;

public class HavingSymbolValidator {

    private final static InnerValidator INNER_VALIDATOR = new InnerValidator();

    public static void validate(Symbol symbol, @Nullable List<Symbol> groupBySymbols) throws IllegalArgumentException {
        INNER_VALIDATOR.process(symbol, new HavingContext(groupBySymbols));
    }

    static class HavingContext {
        @Nullable
        private final List<Symbol> groupBySymbols;

        private boolean insideAggregation = false;

        public HavingContext(@Nullable List<Symbol> groupBySymbols) {
            this.groupBySymbols = unwrap(groupBySymbols);
        }
    }

    private static class InnerValidator extends SymbolVisitor<HavingContext, Void> {

        @Override
        public Void visitReference(Reference symbol, HavingContext context) {
            if (!context.insideAggregation && (context.groupBySymbols == null || !context.groupBySymbols.contains(symbol))) {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("Cannot use reference %s outside of an Aggregation in HAVING clause. " +
                                "Only GROUP BY keys allowed here.", symbol));
            }
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, HavingContext context) {
            if (symbol.info().type().equals(FunctionInfo.Type.AGGREGATE)) {
                context.insideAggregation = true;
            }
            for (Symbol argument : symbol.arguments()) {
                process(argument, context);
            }
            context.insideAggregation = false;
            return null;
        }

        @Override
        public Void visitField(Field field, HavingContext context) {
            return process(field.target(), context);
        }

        @Override
        protected Void visitSymbol(Symbol symbol, HavingContext context) {
            return null;
        }
    }
}
/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.symbol;

import io.crate.metadata.FunctionInfo;

public final class Aggregations {

    private final static AggregationSearcher AGGREGATION_SEARCHER = new AggregationSearcher();

    /**
     * @return true if the symbol is an aggregation or function which contains an aggregation.
     */
    public static boolean containsAggregation(Symbol s) {
        return AGGREGATION_SEARCHER.process(s, null);
    }

    private static class AggregationSearcher extends SymbolVisitor<Void, Boolean> {

        @Override
        protected Boolean visitSymbol(Symbol symbol, Void context) {
            return false;
        }

        @Override
        public Boolean visitFunction(Function symbol, Void context) {
            if (symbol.info().type() == FunctionInfo.Type.AGGREGATE) {
                return true;
            } else {
                for (Symbol argument : symbol.arguments()) {
                    if (process(argument, context)) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Boolean visitAggregation(Aggregation symbol, Void context) {
            return true;
        }
    }
}


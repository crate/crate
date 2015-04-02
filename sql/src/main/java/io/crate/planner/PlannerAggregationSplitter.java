/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner;

import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;

class PlannerAggregationSplitter extends SymbolVisitor<PlannerContext, Symbol> {

    @Override
    public Symbol visitFunction(Function function, PlannerContext context) {
        Symbol result = function;
        if (function.info().type() == FunctionInfo.Type.AGGREGATE) {
            result = new InputColumn(context.numGroupKeys + context.aggregations.size());
            Aggregation aggregation = new Aggregation(function.info(), function.arguments(),
                    Aggregation.Step.ITER, context.step());
            context.aggregations.add(aggregation);
            context.parent = aggregation;
        }

        int idx = 0;
        for (Symbol symbol : function.arguments()) {
            function.arguments().set(idx, process(symbol, context));
            idx++;
        }
        return result;
    }

    @Override
    public Symbol visitReference(Reference symbol, PlannerContext context) {
        if (context.parent != null && context.parent.symbolType() == SymbolType.AGGREGATION) {
            return context.allocateToCollect(symbol);
        }
        return symbol;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, PlannerContext context) {
        return symbol;
    }
}

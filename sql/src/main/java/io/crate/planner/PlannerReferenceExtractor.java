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

package io.crate.planner;

import io.crate.planner.symbol.*;

public class PlannerReferenceExtractor extends SymbolVisitor<PlannerContext,Symbol> {
    @Override
    public Symbol visitFunction(Function function, PlannerContext context) {
        for (int i = 0, size = function.arguments().size(); i < size; i++) {
            function.arguments().set(i, process(function.arguments().get(i), context));
        }
        return function;
    }

    @Override
    public Symbol visitReference(Reference symbol, PlannerContext context) {
        return context.allocateToCollect(symbol);
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference symbol, PlannerContext context) {
        return context.allocateToCollect(symbol);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, PlannerContext context) {
        return symbol;
    }
}

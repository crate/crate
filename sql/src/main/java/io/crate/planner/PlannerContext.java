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

import com.google.common.collect.ImmutableList;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolFormatter;

import java.util.*;

public class PlannerContext {

    final int numGroupKeys;
    List<Aggregation> aggregations = new ArrayList<>();
    List<Symbol> groupBy;
    Aggregation.Step[] steps;
    int stepIdx = 0;
    Symbol parent;
    Map<Symbol, InputColumn> toCollectAllocation = new LinkedHashMap<>();
    Map<Symbol, Symbol> resolvedSymbols = new HashMap<>();
    List<Symbol> orderBy = new ArrayList<>();
    List<Symbol> outputs = new ArrayList<>();
    List<Symbol> originalGroupBy;
    ImmutableList.Builder<Projection> projectionBuilder;
    Map<Symbol, Symbol> currentResolvedSymbols = new HashMap<>();

    public PlannerContext(int numGroupKeys, int numAggregationSteps) {
        this.numGroupKeys = numGroupKeys;
        this.groupBy = new ArrayList<>(numGroupKeys);
        switch (numAggregationSteps) {
            case 0:
                this.steps = null;
                break;
            case 1:
                this.steps = new Aggregation.Step[]{Aggregation.Step.FINAL};
                break;
            case 2:
                this.steps = new Aggregation.Step[]{Aggregation.Step.PARTIAL, Aggregation.Step.FINAL};
                break;
            default:
                throw new IllegalArgumentException("Invalid number of aggregation steps");
        }
    }

    public Aggregation.Step step() {
        return steps[stepIdx];
    }

    Symbol allocateToCollect(Symbol symbol) {

        // handle the case that we got 1 function twice
        // symbol is already an InputColumn
        if (symbol instanceof InputColumn) {
            if (toCollectAllocation.containsValue(symbol)) {
                return symbol;
            } else {
                throw new IllegalArgumentException(
                        SymbolFormatter.format("Symbol %s cannot be collected.", symbol));
            }
        }
        InputColumn inputColumn = toCollectAllocation.get(symbol);
        if (inputColumn == null) {
            inputColumn = new InputColumn(toCollectAllocation.size());
            toCollectAllocation.put(symbol, inputColumn);
        }
        return inputColumn;
    }

    void addResolvedSymbol(Symbol original, Symbol resolved) {
        resolvedSymbols.put(original, resolved);
        currentResolvedSymbols.put(original, resolved);
    }
}

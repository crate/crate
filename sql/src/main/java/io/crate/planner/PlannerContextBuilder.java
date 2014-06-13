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
import com.google.common.collect.Lists;
import io.crate.exceptions.UnhandledServerException;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PlannerContextBuilder {

    private final PlannerContext context;
    public final boolean ignoreOrderBy;
    public boolean aggregationsWrappedInScalar;

    public PlannerContextBuilder() {
        this(0, ImmutableList.<Symbol>of(), false);
    }

    public PlannerContextBuilder(int numAggregationSteps) {
        this(numAggregationSteps, ImmutableList.<Symbol>of(), false);
    }

    public PlannerContextBuilder(int numAggregationSteps, List<Symbol> groupBy) {
        this(numAggregationSteps, groupBy, false);
    }

    public PlannerContextBuilder(int numAggregationSteps, List<Symbol> groupBy, boolean ignoreOrderBy) {
        this.context = new PlannerContext(groupBy.size(), numAggregationSteps);
        context.originalGroupBy = groupBy;
        for (Symbol symbol : groupBy) {
            context.groupBy.add(context.allocateToCollect(symbol));
        }
        this.ignoreOrderBy = ignoreOrderBy;
    }

    public List<Symbol> groupBy() {
        return context.groupBy;
    }

    public List<Symbol> toCollect() {
        return Lists.newArrayList(context.toCollectAllocation.keySet());
    }

    public PlannerContextBuilder searchOutput(List<Symbol> symbols) {
        for (Symbol symbol : symbols) {
            context.outputs.add(Planner.referenceExtractor.process(symbol, context));
        }
        return this;
    }

    /**
     * calculates the toCollect symbols and generates the outputs which can be used
     * for the first TopNProjection
     * <p/>
     * to build AggregationProjection or GroupProjections use the groupBy or aggregations symbols directly.
     */
    public PlannerContextBuilder output(List<Symbol> symbols) {
        if (context.steps == null) {
            for (Symbol symbol : symbols) {
                context.outputs.add(context.allocateToCollect(symbol));
            }
        } else {
            // need to split on aggregations
            for (Symbol symbol : symbols) {
                context.parent = null;
                Symbol splitSymbol = Planner.splitter.process(symbol, context);
                Symbol resolvedSymbol;
                if (context.parent != null && splitSymbol.symbolType() == SymbolType.FUNCTION) {
                    // found a scalar function with an aggregation underneath.
                    aggregationsWrappedInScalar = true;
                    resolvedSymbol = splitSymbol;
                } else if (context.resolvedSymbols.containsKey(splitSymbol)) {
                    // could be a re-occurring "count(distinct <col>)"
                    resolvedSymbol = splitSymbol;
                } else if (context.numGroupKeys > 0 && splitSymbol.symbolType() != SymbolType.INPUT_COLUMN) {
                    // in case the symbol was an aggregation function it is replaced directly.
                    // this wasn't the case so the symbol must be a group by
                    resolvedSymbol = new InputColumn(context.originalGroupBy.indexOf(splitSymbol));
                } else if (splitSymbol.symbolType() == SymbolType.INPUT_COLUMN) {
                    resolvedSymbol = splitSymbol;
                } else if(symbol.symbolType().isValueSymbol()){
                    resolvedSymbol = symbol;
                } else {
                    throw new UnhandledServerException(
                            "Unexpected result column symbol: " + symbol);
                }

                context.resolvedSymbols.put(symbol, resolvedSymbol);
                context.outputs.add(resolvedSymbol);
            }
        }
        return this;
    }

    public List<Aggregation> aggregations() {
        return Lists.newArrayList(context.aggregations);
    }

    /**
     * this can be used after the first GroupProjection or AggregationProjection
     * it will move the groupBy and aggregations inputColumns to point to the outputs of the previous projection
     */
    public void nextStep() {
        context.stepIdx++;
        if (context.stepIdx + 1 <= context.steps.length) {
            // aggregations now take the partial aggregation state as their input.
            int idx = context.numGroupKeys;
            int i = 0;
            for (Aggregation aggregation : context.aggregations) {
                context.aggregations.set(i, new Aggregation(
                        aggregation.functionInfo(),
                        Arrays.<Symbol>asList(new InputColumn(idx)),
                        aggregation.toStep(), context.step()));
                i++;
                idx++;
            }
        }

        int idx = 0;
        for (Symbol symbol : context.groupBy) {
            context.groupBy.set(idx, new InputColumn(idx));
        }
    }

    public List<Symbol> orderBy() {
        return ignoreOrderBy ? ImmutableList.<Symbol>of() : Lists.newArrayList(context.orderBy);
    }

    public PlannerContextBuilder orderBy(List<Symbol> symbols) {
        if (symbols == null || ignoreOrderBy) {
            return this;
        }
        if (context.steps == null || context.numGroupKeys == 0) {
            for (Symbol symbol : symbols) {
                context.orderBy.add(context.allocateToCollect(symbol));
            }
        } else {
            for (Symbol symbol : symbols) {
                Symbol resolvedSymbol = context.resolvedSymbols.get(symbol);

                // symbol is already resolved at this point
                // because outputs are set first and in the group by case
                // every symbol in order by must be present in the outputs

                if (resolvedSymbol == null) {
                    throw new UnsupportedOperationException(
                        String.format("%s must be in the result column list in order to order by it", symbol));
                }
                context.orderBy.add(resolvedSymbol);
            }
        }
        return this;
    }

    /**
     * returns the symbols to be used in the first topN projection
     * <p/>
     * if their is a second topN projection the {@link #passThroughOutputs()} method should be
     * used.
     */
    public List<Symbol> outputs() {
        return Lists.newArrayList(context.outputs);
    }

    /**
     * use this together with {@link #passThroughOutputs()}
     * in the topN node
     * <p/>
     * if {@link #outputs()} is used use {@link #orderBy()} instead.
     *
     * @return
     */
    public List<Symbol> passThroughOrderBy() {
        List<Symbol> orderBy = new ArrayList<>();
        for (Symbol symbol : context.orderBy) {
            orderBy.add(new InputColumn(context.outputs.indexOf(symbol)));
        }

        return orderBy;
    }

    /**
     * will just take the inputs as they are. No re-ordering or scalar functions are left at this point.
     * Make sure to use {@link #outputs()} before this is used..
     */
    public List<Symbol> passThroughOutputs() {
        List<Symbol> outputs = new ArrayList<>();
        for (int i = 0; i < context.outputs.size(); i++) {
            outputs.add(new InputColumn(i));
        }
        return outputs;
    }
}

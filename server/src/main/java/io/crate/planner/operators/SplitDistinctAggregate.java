/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.operators;

import static io.crate.analyze.expressions.ExpressionAnalyzer.allocateFunction;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.UnaryOperator;

import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.execution.engine.aggregation.sum.SumAggregation;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;

/// Splits a
public final class SplitDistinctAggregate {

    /// @param partials     aggregates for the inner `GroupHashAggregate`, over the raw source columns.
    ///                     Empty for a `DISTINCT` aggregate: the `GROUP BY` key already de-duplicates it.
    /// @param combiners    outer combiner aggregates for the `AggregationProjection`, which combine `partials` into
    ///                     a global value (e.g. `sum` over per-group `sum(x)`/`count(x)` partials).
    ///                     For a `DISTINCT` aggregate, this is just the original function with `distinct` cleared, run
    ///                     directly on the deduped rows.
    /// @param outputs      one expression per original aggregate, built from `combiners`. Only `AVG` needs
    ///                     a real expression (`divide(...)`) -- every other function just passes through.
    private record Split(List<Function> partials, List<Function> combiners, List<Symbol> outputs) {}

    private SplitDistinctAggregate() {
    }

    /// Returns the `partials` for the inner `GroupHashAggregate`; see [Split] for details.
    /// Returns `null` if `aggregates` can't be split safely.
    /// Used by [io.crate.planner.optimizer.rule.RewriteMixedDistinctAggToGroupBy] to build the `GroupHashAggregate`.
    @Nullable
    public static List<Function> partials(List<Function> aggregates, TransactionContext txnCtx, NodeContext nodeCtx) {
        Split split = split(aggregates, txnCtx, nodeCtx);
        return split == null ? null : split.partials();
    }

    /// Rewrite the given `aggregates`, containing distinct and non-distinct functions,
    /// to functions that can be executed in groups (`GroupHashAggregate`s), and then
    /// functions that calculate the final output using the partial values from the groups.
    /// For example, for `avg(x)`, we calculate `sum(x)` and `count(x)` for each group.
    /// The final output is `sum(sum(x)) / sum(count(x))`.
    static DistinctRewriter.Result rewriteForGroupBy(List<Function> aggregates,
                                                UnaryOperator<Symbol> paramBinder,
                                                TransactionContext txnCtx,
                                                NodeContext nodeCtx) {
        Split split = split(aggregates, txnCtx, nodeCtx);
        assert split != null
            : "RewriteMixedDistinctAggToGroupBy already proved this split is safe for " + aggregates;

        List<Symbol> outputsBound = Lists.map(split.outputs(), paramBinder);
        List<Symbol> reduceBound = Lists.map(split.combiners(), paramBinder);
        var evalProj = new EvalProjection(
            InputColumns.create(outputsBound, new InputColumns.SourceSymbols(reduceBound))
        );
        return new DistinctRewriter.Result(split.combiners(), split.combiners(), evalProj);
    }

    @Nullable
    private static Split split(List<Function> aggregates, TransactionContext txnCtx, NodeContext nodeCtx) {
        List<Function> partials = new ArrayList<>();
        List<Function> outerAggregates = new ArrayList<>();
        List<Symbol> outputs = new ArrayList<>();

        try {
            for (Function fn : aggregates) {
                if (fn.distinct()) {
                    // e.g. count(DISTINCT y) -> count(y): rows are already unique per `y` after the GROUP BY below
                    Function outer = new Function(fn.signature(), fn.arguments(), fn.valueType(), fn.filter(), false);
                    outerAggregates.add(outer);
                    outputs.add(outer);
                } else if (isAvg(fn.name())) {
                    Function sumPartial = allocateFunction(SumAggregation.NAME, fn.arguments(), null, null, txnCtx, nodeCtx);
                    Function countPartial = allocateFunction(CountAggregation.NAME, fn.arguments(), null, null, txnCtx, nodeCtx);
                    partials.add(sumPartial);
                    partials.add(countPartial);

                    Function sumTotal = allocateFunction(SumAggregation.NAME, List.<Symbol>of(sumPartial), null, null, txnCtx, nodeCtx);
                    Function countTotal = allocateFunction(SumAggregation.NAME, List.<Symbol>of(countPartial), null, null, txnCtx, nodeCtx);
                    outerAggregates.add(sumTotal);
                    outerAggregates.add(countTotal);

                    Symbol sumCast = sumTotal.cast(fn.valueType(), CastMode.IMPLICIT);
                    Symbol countCast = countTotal.cast(fn.valueType(), CastMode.IMPLICIT);
                    outputs.add(allocateFunction(
                        ArithmeticFunctions.Names.DIVIDE, List.of(sumCast, countCast), null, null, txnCtx, nodeCtx));
                } else {
                    Function partial = allocateFunction(fn.name(), fn.arguments(), null, null, txnCtx, nodeCtx);
                    partials.add(partial);

                    // count(inner) rolls up via sum(); sum/min/max roll up via themselves.
                    String combiner = fn.name().equals(CountAggregation.NAME) ? SumAggregation.NAME : fn.name();
                    Function outer = allocateFunction(combiner, List.<Symbol>of(partial), null, null, txnCtx, nodeCtx);
                    outerAggregates.add(outer);
                    outputs.add(outer);
                }
            }
        } catch (UnsupportedFunctionException e) {
            // e.g. avg(interval)/avg(timestamptz): sum() doesn't support the argument type, so the split isn't safe.
            return null;
        }

        for (int i = 0; i < outputs.size(); i++) {
            if (!outputs.get(i).valueType().equals(aggregates.get(i).valueType())) {
                // Splitting changed the result type somewhere (e.g. integer division) -- don't risk it.
                return null;
            }
        }

        return new Split(partials, List.copyOf(new LinkedHashSet<>(outerAggregates)), outputs);
    }

    private static boolean isAvg(String name) {
        return name.equals(AverageAggregation.NAMES[0]) || name.equals(AverageAggregation.NAMES[1]);
    }
}

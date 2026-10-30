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

import static io.crate.analyze.expressions.ExpressionAnalyzer.allocateBuiltinOrUdfFunction;
import static io.crate.analyze.expressions.ExpressionAnalyzer.allocateFunction;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;

import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.execution.engine.aggregation.impl.CountAggregation;
import io.crate.execution.engine.aggregation.impl.average.AverageAggregation;
import io.crate.execution.engine.aggregation.sum.SumAggregation;
import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.NodeContext;
import io.crate.metadata.TransactionContext;

/// Rewrites distinct aggregate functions in `aggregates`/`outputs` so that they can be used in
/// aggregation and group projections. Implementations differ in strategy:
///  - [CollectSet]: the default rewrite, replacing distinct functions with `collect_set()`.
///  - [GroupByPartials]: splits a global aggregate that mixes distinct and non-distinct aggregates over
///    different columns into partials for a `GroupHashAggregate`.
///  - [NoOp]: `source` already deduplicates the distinct argument, so no rewrite is needed.
public sealed interface DistinctRewriter {
    record Result(
        List<Function> aggregates,
        List<? extends Symbol> outputs,
        @Nullable EvalProjection evalProjection
    ) {}

    Result rewrite(List<Function> aggregates,
                   List<? extends Symbol> outputs,
                   UnaryOperator<Symbol> paramBinder,
                   TransactionContext txnCtx,
                   NodeContext nodeCtx);

    /// Implements the default rewrite of distinct functions in global and group aggregate operators, which is to
    /// replace them with `collect_set()` (in aggregation/group projections) and collection_agg_func(collect_set(x))`
    /// (in a final `EvalProjection`).
    /// The visitor context is the rewrite that [#visitFunction] applies to a distinct
    /// function, so either [#toCollectSet] or [#toCollectionFunction].
    final class CollectSet extends SymbolVisitor<UnaryOperator<Function>, Symbol> implements DistinctRewriter {

        private TransactionContext txnCtx;
        private NodeContext nodeCtx;

        /// Rewrites the given `aggregates` and `outputs` so that they can be used in aggregation and group projections.
        /// Also returns a matching [EvalProjection].
        ///
        /// Distinct functions in `aggregates` and `outputs` are replaced with `collect_set(x)`.
        /// This is the form used for aggregation/group projections in global/group aggregate operators.
        ///
        /// The returned `EvalProjection` contains a `collection_agg_func(collect_set(x))` for each distinct `agg_func()`
        /// in given `outputs`. For example, `count(distinct x)` becomes `collection_count(collect_set(x))`.
        /// @param aggregates Aggregate functions to be rewritten.
        /// @param outputs Output symbols to be rewritten. Same as `aggregates` for global aggregations.
        ///                Group aggregations also add the grouping keys to `outputs`.
        @Override
        public Result rewrite(List<Function> aggregates,
                              List<? extends Symbol> outputs,
                              UnaryOperator<Symbol> paramBinder,
                              TransactionContext txnCtx,
                              NodeContext nodeCtx) {
            // If there are no distinct functions, return aggregates/outputs as they are.
            boolean noneDistinct = aggregates.stream()
                .noneMatch(agg -> agg.any(sym -> sym instanceof Function fn && fn.distinct()));

            if (noneDistinct) {
                return new Result(aggregates, outputs, null);
            }

            this.txnCtx = txnCtx;
            this.nodeCtx = nodeCtx;

            List<Function> aggregatesCollectSet = rewrite(aggregates, this::toCollectSet);
            List<? extends Symbol> outputsCollectSet = rewrite(outputs, this::toCollectSet);

            List<? extends Symbol> outputsCollectSetBound = Lists.map(outputsCollectSet, paramBinder);
            List<? extends Symbol> outputsCollectionFuncsBound = Lists.map(rewrite(outputs, this::toCollectionFunction), paramBinder);

            var evalProj = new EvalProjection(
                InputColumns.create(outputsCollectionFuncsBound, new InputColumns.SourceSymbols(outputsCollectSetBound))
            );

            return new Result(aggregatesCollectSet, outputsCollectSet, evalProj);
        }

        // Safe because every visitXYZ() method returns a symbol of the same type.
        @SuppressWarnings("unchecked")
        private <T extends Symbol> List<T> rewrite(List<T> symbols, UnaryOperator<Function> rewriteDistinct) {
            return Lists.map(symbols, symbol -> (T) symbol.accept(this, rewriteDistinct));
        }

        @Override
        public Symbol visitAlias(AliasSymbol aliasSymbol, UnaryOperator<Function> rewriteDistinct) {
            Symbol rewritten = aliasSymbol.symbol().accept(this, rewriteDistinct);
            return rewritten == aliasSymbol.symbol()
                ? aliasSymbol
                : new AliasSymbol(aliasSymbol.alias(), rewritten);
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, UnaryOperator<Function> rewriteDistinct) {
            return symbol;
        }

        @Override
        public Symbol visitFunction(Function fn, UnaryOperator<Function> rewriteDistinct) {
            boolean changed = false;
            List<Symbol> newArgs = new ArrayList<>(fn.arguments().size());
            for (Symbol arg : fn.arguments()) {
                Symbol rewritten = arg.accept(this, rewriteDistinct);
                changed |= rewritten != arg;
                newArgs.add(rewritten);
            }

            // Keep `fn` when nothing was rewritten.
            // `rewrite()` builds the `EvalProjection` from `InputColumns.SourceSymbols`,
            // which keys non-deterministic functions by identity.
            if (!fn.distinct()) {
                return changed
                    ? new Function(fn.signature(), newArgs, fn.valueType(), fn.filter(), false)
                    : fn;
            }

            return rewriteDistinct.apply(new Function(
                fn.signature(),
                newArgs,
                fn.valueType(),
                fn.filter(),
                // the rewrite replaces the function, so the flag is not needed anymore
                false
            ));
        }

        /// `agg_func(distinct x)` -> `collection_agg_func(collect_set(x))`. Only aggregates with a matching
        /// `collection_*` scalar are supported, e.g. `count` and `avg`; anything else throws.
        private Function toCollectionFunction(Function original) {
            String name = original.name();

            String collectionFuncName = "collection_" + name;
            List<Symbol> args = List.of(toCollectSet(original));
            try {
                // No window definition or ignore-nulls flag is passed on, because a `WindowFunction` is
                // always built with `distinct = false` and therefore never reaches this method.
                return allocateBuiltinOrUdfFunction(
                    original.signature().getName().schema(),
                    collectionFuncName,
                    args,
                    null,
                    null,
                    null,
                    false,
                    null,
                    txnCtx,
                    nodeCtx
                );
            } catch (UnsupportedOperationException ex) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "unknown function %s(DISTINCT %s)",
                    name,
                    original.arguments().get(0).valueType()), ex
                );
            }
        }

        /// `count(distinct x)` -> `collect_set(x)`
        private Function toCollectSet(Function original) {
            return allocateFunction(
                CollectSetAggregation.NAME,
                original.arguments(),
                original.filter(),
                null,
                txnCtx,
                nodeCtx
            );
        }
    }

    /// Rewrites a global aggregate that mixes distinct and non-distinct aggregates over different
    /// columns into partials for a `GroupHashAggregate`. See [io.crate.planner.optimizer.rule.RewriteMixedDistinctAggToGroupBy].
    final class GroupByPartials implements DistinctRewriter {

        /// @param partials     aggregates for the inner `GroupHashAggregate`, over the raw source columns.
        ///                     Empty for a `DISTINCT` aggregate because the `GROUP BY` key already de-duplicates it.
        /// @param combiners    outer combiner aggregates for the `AggregationProjection`, which combine `partials` into
        ///                     a global value. For example, a global `sum(x)` needs a `sum` combines over each group's
        ///                     `sum(x)` partial.
        ///                     For a `DISTINCT` aggregate, this is just the original function with `distinct` cleared,
        ///                     because it runs directly on the deduped rows within a group.
        /// @param outputs      one expression per original aggregate, built from `combiners`. Only `AVG` needs
        ///                     a real expression (`divide(...)`) -- every other function just passes through.
        private record Split(List<Function> partials, List<Function> combiners, List<Symbol> outputs) {}

        /// Returns the `partials` to be used in the inner `GroupHashAggregate`; see [Split] for details.
        /// Returns `null` if `aggregates` can't be split safely.
        @Nullable
        public List<Function> partials(List<Function> aggregates, TransactionContext txnCtx, NodeContext nodeCtx) {
            return split(aggregates, txnCtx, nodeCtx).partials();
        }

        /// Rewrite the given `aggregates`, containing distinct and non-distinct functions,
        /// to functions that can be executed in groups (`GroupHashAggregate`s), and then
        /// functions that calculate the final output using the partial values from the groups.
        /// For example, for `avg(x)`, we calculate `sum(x)` and `count(x)` for each group.
        /// The final output is `sum(sum(x)) / sum(count(x))`.
        @Override
        public Result rewrite(List<Function> aggregates,
                              List<? extends Symbol> outputs,
                              UnaryOperator<Symbol> paramBinder,
                              TransactionContext txnCtx,
                              NodeContext nodeCtx) {
            Split split = split(aggregates, txnCtx, nodeCtx);

            List<Symbol> outputsBound = Lists.map(split.outputs(), paramBinder);
            List<Symbol> reduceBound = Lists.map(split.combiners(), paramBinder);
            var evalProj = new EvalProjection(
                InputColumns.create(outputsBound, new InputColumns.SourceSymbols(reduceBound))
            );
            return new Result(split.combiners(), split.combiners(), evalProj);
        }

        private static Split split(List<Function> aggregates, TransactionContext txnCtx, NodeContext nodeCtx) {
            List<Function> partials = new ArrayList<>();
            List<Function> combiners = new ArrayList<>();
            List<Symbol> outputs = new ArrayList<>();

            for (Function fn : aggregates) {
                if (fn.distinct()) {
                    // e.g. count(DISTINCT y) -> count(y): rows are already unique per `y` after the GROUP BY below
                    Function outer = new Function(fn.signature(), fn.arguments(), fn.valueType(), fn.filter(), false);
                    combiners.add(outer);
                    outputs.add(outer);
                } else if (isAvg(fn.name())) {
                    // For `avg(x)`, we calculate `sum(x)` and `count(x)` for each group.
                    // The final output is `sum(sum(x)) / sum(count(x))`.
                    Function sumPartial = allocateFunction(SumAggregation.NAME, fn.arguments(), null, null, txnCtx, nodeCtx);
                    Function countPartial = allocateFunction(CountAggregation.NAME, fn.arguments(), null, null, txnCtx, nodeCtx);
                    partials.add(sumPartial);
                    partials.add(countPartial);

                    Function sumTotal = allocateFunction(SumAggregation.NAME, List.of(sumPartial), null, null, txnCtx, nodeCtx);
                    Function countTotal = allocateFunction(SumAggregation.NAME, List.of(countPartial), null, null, txnCtx, nodeCtx);
                    combiners.add(sumTotal);
                    combiners.add(countTotal);

                    Symbol sumCast = sumTotal.cast(fn.valueType(), CastMode.IMPLICIT);
                    Symbol countCast = countTotal.cast(fn.valueType(), CastMode.IMPLICIT);
                    outputs.add(allocateFunction(
                        ArithmeticFunctions.Names.DIVIDE, List.of(sumCast, countCast), null, null, txnCtx, nodeCtx));
                } else {
                    Function partial = allocateFunction(fn.name(), fn.arguments(), null, null, txnCtx, nodeCtx);
                    partials.add(partial);

                    // A global count() = sum(count(inner));
                    // sum/min/max roll up via themselves, e.g. a global sum() = sum(sum(inner))
                    String combiner = fn.name().equals(CountAggregation.NAME) ? SumAggregation.NAME : fn.name();
                    Function outer = allocateFunction(combiner, List.of(partial), null, null, txnCtx, nodeCtx);
                    combiners.add(outer);
                    outputs.add(outer);
                }
            }

            return new Split(partials, List.copyOf(new LinkedHashSet<>(combiners)), outputs);
        }

        private static boolean isAvg(String name) {
            return name.equals(AverageAggregation.NAMES[0]) || name.equals(AverageAggregation.NAMES[1]);
        }
    }

    /// No rewrite needed: `source` already deduplicates the distinct argument.
    /// See [io.crate.planner.optimizer.rule.RewriteDistinctAggToGroupBy].
    final class NoOp implements DistinctRewriter {
        @Override
        public Result rewrite(List<Function> aggregates,
                              List<? extends Symbol> outputs,
                              UnaryOperator<Symbol> paramBinder,
                              TransactionContext txnCtx,
                              NodeContext nodeCtx) {
            return new Result(aggregates, outputs, null);
        }
    }
}

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
import java.util.List;
import java.util.Locale;
import java.util.function.UnaryOperator;

import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;

/// Implements the default rewrite of distinct functions in global and group aggregate operators, which is to replace
/// them with `collect_set()` (in aggregation/group projections) and `collection_agg_func(collect_set(x))` (in a final
/// `EvalProjection`).
/// The visitor context is the rewrite that [#visitFunction] applies to a distinct function, so either
/// [#toCollectSet] or [#toCollectionFunction].
class DistinctRewriter extends SymbolVisitor<UnaryOperator<Function>, Symbol> {
    record Result(
        List<Function> aggregates,
        List<? extends Symbol> outputs,
        @Nullable EvalProjection evalProjection
    ) {}

    private final CoordinatorTxnCtx txnCtx;
    private final NodeContext nodeCtx;

    private DistinctRewriter(CoordinatorTxnCtx txnCtx, NodeContext nodeCtx) {
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
    }

    /// Rewrites the given `aggregates` and `outputs` so that they can be used in aggregation and group projections.
    /// Also returns a matching [EvalProjection].
    ///
    /// Distinct functions in `aggregates` and `outputs` are replaced with `collect_set(x)`.
    /// This is the form used for aggregation/group projections in global/group aggregate operators.
    ///
    /// The returned `EvalProjection` contains a `collection_agg_func(collect_set(x))` for each distinct `agg_func()`
    /// in given `outputs`. For example, `count(distinct x)` becomes `collection_count(collect_set(x))`.
    /// @param aggregates Aggregate functions to be rewritten.
    /// @param outputs Output symbols to be rewritten. Same as `aggregates` for global aggregations. Group aggregations
    ///                also add the grouping keys to `outputs`.
    static Result rewrite(List<Function> aggregates,
                          List<? extends Symbol> outputs,
                          UnaryOperator<Symbol> paramBinder,
                          CoordinatorTxnCtx txnCtx,
                          NodeContext nodeCtx) {
        // If there are no distinct functions, return aggregates/outputs as they are.
        boolean noneDistinct = aggregates.stream()
            .noneMatch(agg -> agg.any(sym -> sym instanceof Function fn && fn.distinct()));

        if (noneDistinct) {
            return new Result(aggregates, outputs, null);
        }

        var rewriter = new DistinctRewriter(txnCtx, nodeCtx);
        List<Function> aggregatesCollectSet = rewriter.rewrite(aggregates, rewriter::toCollectSet);
        List<? extends Symbol> outputsCollectSet = rewriter.rewrite(outputs, rewriter::toCollectSet);

        List<? extends Symbol> outputsCollectSetBound = Lists.map(outputsCollectSet, paramBinder);
        List<? extends Symbol> outputsCollectionFuncsBound = Lists.map(rewriter.rewrite(outputs, rewriter::toCollectionFunction), paramBinder);

        var evalProj = new EvalProjection(
            InputColumns.create(outputsCollectionFuncsBound, new InputColumns.SourceSymbols(outputsCollectSetBound))
        );

        return new Result(aggregatesCollectSet, outputsCollectSet, evalProj);
    }

    /// Returns the `aggregates` unchanged, for callers that don't need a rewrite.
    static Result noop(List<Function> aggregates) {
        return new Result(aggregates, aggregates, null);
    }

    /// Replaces distinct functions with the aggregate that collects their values.
    /// `count(distinct x)` -> `collect_set(x)`
    private static <T extends Symbol> List<T> toCollectSet(List<T> symbols,
                                                   CoordinatorTxnCtx txnCtx,
                                                   NodeContext nodeCtx) {
        var rewriter = new DistinctRewriter(txnCtx, nodeCtx);
        return rewriter.rewrite(symbols, rewriter::toCollectSet);
    }

    /// Replaces distinct functions with the scalar applied over the collected values.
    /// `count(distinct x)` -> `collection_count(collect_set(x))`
    private static <T extends Symbol> List<T> toCollectionFunctions(List<T> symbols,
                                                                    CoordinatorTxnCtx txnCtx,
                                                                    NodeContext nodeCtx) {
        var rewriter = new DistinctRewriter(txnCtx, nodeCtx);
        return rewriter.rewrite(symbols, rewriter::toCollectionFunction);
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

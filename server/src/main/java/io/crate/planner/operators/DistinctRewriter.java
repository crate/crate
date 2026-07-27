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

import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;

public class DistinctRewriter extends SymbolVisitor<Object, Symbol> {

    private final CoordinatorTxnCtx txnCtx;
    private final NodeContext nodeCtx;
    private final boolean collectionSetOnly;

    private DistinctRewriter(CoordinatorTxnCtx txnCtx, NodeContext nodeCtx, boolean collectionSetOnly) {
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
        this.collectionSetOnly = collectionSetOnly;
    }

    /// Replaces distinct functions with the aggregate that collects their values.
    /// `count(distinct x)` -> `collect_set(x)`
    ///
    /// This is the form the aggregation projections consume, so it is used by the aggregate
    /// operators and by consumers that need to match against what those operators emit.
    public static <T extends Symbol> List<T> toCollectSet(List<T> symbols,
                                                          CoordinatorTxnCtx txnCtx,
                                                          NodeContext nodeCtx) {
        return new DistinctRewriter(txnCtx, nodeCtx, true).rewrite(symbols);
    }

    /// Replaces distinct functions with the scalar applied over the collected values.
    /// `count(distinct x)` -> `collection_count(collect_set(x))`
    ///
    /// This is the finalized form, evaluated on top of what [#toCollectSet] produced.
    public static <T extends Symbol> List<T> toCollectionFunctions(List<T> symbols,
                                                                   CoordinatorTxnCtx txnCtx,
                                                                   NodeContext nodeCtx) {
        return new DistinctRewriter(txnCtx, nodeCtx, false).rewrite(symbols);
    }

    /// Safe because every branch of [#visitFunction] returns a [Function], and any symbol the
    /// visitor does not handle is returned unchanged.
    @SuppressWarnings("unchecked")
    private <T extends Symbol> List<T> rewrite(List<T> symbols) {
        return symbols.stream()
            .map(symbol -> (T) symbol.accept(this, null))
            .toList();
    }

    @Override
    public Symbol visitAlias(AliasSymbol aliasSymbol, Object context) {
        Symbol rewritten = aliasSymbol.symbol().accept(this, context);
        return rewritten == aliasSymbol.symbol()
            ? aliasSymbol
            : new AliasSymbol(aliasSymbol.alias(), rewritten);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Object context) {
        return symbol;
    }

    /// The default implementation unwraps the marker into its `_fetchid` reference.
    @Override
    public Symbol visitFetchMarker(FetchMarker fetchMarker, Object context) {
        return fetchMarker;
    }

    @Override
    public Symbol visitFunction(Function fn, Object context) {
        boolean changed = false;
        List<Symbol> newArgs = new ArrayList<>(fn.arguments().size());
        for (Symbol arg : fn.arguments()) {
            Symbol rewritten = arg.accept(this, context);
            changed |= rewritten != arg;
            newArgs.add(rewritten);
        }

        // Return `fn` untouched when nothing was rewritten.
        // Callers such as `Order` and `Eval` rewrite their own outputs and `source.outputs()` separately,
        // and those lists share instances.
        // Returning copies might break lookups between the different collections.
        if (!fn.distinct()) {
            return changed
                ? new Function(fn.signature(), newArgs, fn.valueType(), fn.filter(), false)
                : fn;
        }

        Function newFn = new Function(
            fn.signature(),
            newArgs,
            fn.valueType(),
            fn.filter(),
            // we'll un-distinct it anyway below
            false
        );

        if (collectionSetOnly) {
            return toCollectSet(newFn);
        } else {
            return toCollectionFunction(newFn);
        }
    }

    /// `count(distinct x)` -> `collection_count(collect_set(x))`
    private Function toCollectionFunction(Function original) {
        String name = original.name();
        String schema = original.signature().getName().schema();

        // define the outer function which contains the inner function as argument.
        String nodeName = "collection_" + name;
        var collectSetFn = toCollectSet(original);
        List<Symbol> outerArguments = List.of(collectSetFn);
        try {
            // No window definition or ignore-nulls flag is passed on, because a `WindowFunction` is
            // always built with `distinct = false` and therefore never reaches this method.
            return allocateBuiltinOrUdfFunction(
                schema,
                nodeName,
                outerArguments,
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

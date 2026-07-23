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

import io.crate.analyze.WindowDefinition;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;

public class DistinctRewriter extends DefaultTraversalSymbolVisitor<Object, Symbol> {
    private final CoordinatorTxnCtx txnCtx;
    private final NodeContext nodeCtx;

    public DistinctRewriter(CoordinatorTxnCtx txnCtx, NodeContext nodeCtx) {
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
    }

    public List<Symbol> rewrite(List<Symbol> outputs) {
        return outputs.stream()
            .map((Symbol symbol) -> symbol.accept(DistinctRewriter.this, null))
            .toList();
    }

    public List<Function> rewriteFunctions(List<Function> outputs) {
        return outputs.stream()
            .map((Function fn) -> visitFunction(fn, null))
            .map(Function.class::cast)
            .toList();
    }

    @Override
    public Symbol visitAlias(AliasSymbol aliasSymbol, Object context) {
        return new AliasSymbol(
            aliasSymbol.alias(), aliasSymbol.symbol().accept(this, context)
        );
    }

    @Override
    public Symbol visitReference(Reference symbol, Object context) {
        return symbol;
    }

    @Override
    public Symbol visitFunction(Function fn, Object context) {
        List<Symbol> arguments = new ArrayList<>(fn.arguments().size());
        for (Symbol arg : fn.arguments()) {
            Symbol rewritten = arg.accept(this, context);
            arguments.add(rewritten);
        }

        Function fnNewArgs = new Function(
            fn.signature(),
            arguments,
            fn.valueType(),
            fn.filter(),
            // we'll un-distinct it anyway below
            false
        );

        if (fn.distinct()) {
            return makeCollectSetFunction(fnNewArgs);
        } else {
            return fnNewArgs;
        }
    }

    private Symbol wrapWithCollectionCount(Function original) {
        var arguments = original.arguments();
        ExpressionAnalysisContext context = null;
        String name = original.name();
        WindowDefinition windowDefinition = (original instanceof WindowFunction wf) ? wf.windowDefinition() : null;
        Boolean ignoreNulls = (original instanceof WindowFunction wf) ? wf.ignoreNulls() : null;
        String schema = original.signature().getName().schema();

        // define the outer function which contains the inner function as argument.
        String nodeName = "collection_" + name;
        var collectSetFn = makeCollectSetFunction(original);
        List<Symbol> outerArguments = List.of(collectSetFn);
        try {
            return allocateBuiltinOrUdfFunction(
                schema,
                nodeName,
                outerArguments,
                null,
                ignoreNulls,
                context,
                false,
                windowDefinition,
                txnCtx,
                nodeCtx
            );
        } catch (UnsupportedOperationException ex) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "unknown function %s(DISTINCT %s)", name, arguments.get(0).valueType()), ex);
        }
    }

    public Function makeCollectSetFunction(Function original) {
        var arguments = original.arguments();
        var filter = original.filter();
        ExpressionAnalysisContext context = null;
        Boolean ignoreNulls = (original instanceof WindowFunction wf) ? wf.ignoreNulls() : null;

        return allocateFunction(
            CollectSetAggregation.NAME,
            arguments,
            filter,
            context,
            txnCtx,
            nodeCtx
        );
    }

    private List<Symbol> wrapDistinctWithCollectionCount(List<Symbol> outputs) {
        List<Symbol> list = new ArrayList<>();
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            if (output instanceof Function original && original.distinct()) {
                Symbol collectionCountWrap = wrapWithCollectionCount(original);
                list.add(collectionCountWrap);
            } else {
                list.add(output);
            }
        }
        return list;
    }
}

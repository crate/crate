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

package io.crate.expression.symbol;

import io.crate.metadata.Reference;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Base class which can be used to create a visitor that has to replace functions.
 *
 * @param <C> context class for the visitor.
 */
public abstract class FunctionCopyVisitor<C> extends SymbolVisitor<C, Symbol> {

    /**
     * Traverses the functions arguments using {@link Symbol#accept(SymbolVisitor, Object)}
     * If any process call returns a different instance then a new Function instance is returned.
     */
    protected Function processAndMaybeCopy(Function func, C context) {
        List<Symbol> args = func.arguments();
        switch (args.size()) {
            // specialized functions to avoid allocations for common cases
            case 0:
                return zeroArg(func, context);

            case 1:
                return oneArg(func, context);

            case 2:
                return twoArgs(func, context);

            default:
                return manyArgs(func, context);
        }
    }

    private Function manyArgs(Function func, C context) {
        List<Symbol> args = func.arguments();
        ArrayList<Symbol> newArgs = new ArrayList<>(args.size());

        Symbol filter = func.filter();
        Symbol newFilter = processNullable(filter, context);

        boolean changed = false;
        for (Symbol arg : args) {
            Symbol newArg = requireNonNull(
                arg.accept(this, context),
                "function arguments must never be NULL"
            );
            changed |= arg != newArg;
            newArgs.add(newArg);
        }
        changed |= filter != newFilter;

        if (changed) {
            return new Function(func.info(), func.signature(), newArgs, newFilter);
        }
        return func;
    }

    private Function twoArgs(Function func, C context) {
        assert func.arguments().size() == 2 : "size of arguments must be two";
        Symbol arg1 = func.arguments().get(0);
        Symbol newArg1 = arg1.accept(this, context);
        assert newArg1 != null : arg1 + " became NULL, symbols must never convert to NULL";

        Symbol arg2 = func.arguments().get(1);
        Symbol newArg2 = arg2.accept(this, context);
        assert newArg2 != null : arg2 + " became NULL, symbols must never convert to NULL";

        Symbol filter = func.filter();
        Symbol newFilter = processNullable(filter, context);

        if (arg1 == newArg1 && arg2 == newArg2 && filter == newFilter) {
            return func;
        }
        return new Function(func.info(), func.signature(), List.of(newArg1, newArg2), newFilter);
    }

    private Function zeroArg(Function func, C context) {
        assert func.arguments().size() == 0 : "size of arguments must be zero";

        Symbol filter = func.filter();
        if (filter == null) {
            return func;
        }

        Symbol newFilter = filter.accept(this, context);
        if (filter == newFilter) {
            return func;
        }
        return new Function(func.info(), func.signature(), List.of(), newFilter);
    }

    private Function oneArg(Function func, C context) {
        assert func.arguments().size() == 1 : "size of arguments must be one";
        Symbol arg = func.arguments().get(0);
        Symbol newArg = requireNonNull(arg.accept(this, context), "function arguments must never be NULL");

        Symbol filter = func.filter();
        Symbol newFilter = processNullable(filter, context);

        if (arg == newArg && filter == newFilter) {
            return func;
        }
        return new Function(func.info(), func.signature(), List.of(newArg), newFilter);
    }

    @Nullable
    private Symbol processNullable(@Nullable Symbol symbol, C context) {
        if (symbol != null) {
            return symbol.accept(this, context);
        }
        return null;
    }

    @Override
    public Symbol visitFunction(Function func, C context) {
        return processAndMaybeCopy(func, context);
    }

    @Override
    public Symbol visitWindowFunction(WindowFunction windowFunction, C context) {
        Function processedFunction = processAndMaybeCopy(windowFunction, context);
        return new WindowFunction(
            processedFunction.info(),
            processedFunction.signature(),
            processedFunction.arguments(),
            processNullable(windowFunction.filter(), context),
            windowFunction.windowDefinition().map(s -> s.accept(this, context))
        );
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, C context) {
        Symbol queryTerm = matchPredicate.queryTerm().accept(this, context);
        HashMap<Symbol, Symbol> identBootMap = new HashMap<>();
        for (Map.Entry<Symbol, Symbol> entry : matchPredicate.identBoostMap().entrySet()) {
            identBootMap.put(
                entry.getKey().accept(this, context),
                entry.getValue().accept(this, context)
            );
        }
        return new MatchPredicate(
            identBootMap,
            queryTerm,
            matchPredicate.matchType(),
            matchPredicate.options().accept(this, context)
        );
    }

    @Override
    public Symbol visitAlias(AliasSymbol aliasSymbol, C context) {
        Symbol symbol = aliasSymbol.symbol();
        Symbol newSymbol = symbol.accept(this, context);
        if (symbol == newSymbol) {
            return aliasSymbol;
        } else {
            return new AliasSymbol(aliasSymbol.alias(), newSymbol);
        }
    }

    @Override
    public Symbol visitFetchMarker(FetchMarker fetchMarker, C context) {
        // By default fetchMarker calls visitReference on the inner _fetchId
        // That behavior breaks generic `visitSymbol` replacements
        return visitSymbol(fetchMarker, context);
    }

    @Override
    public Symbol visitFetchStub(FetchStub fetchStub, C context) {
        FetchMarker fetchMarker = fetchStub.fetchMarker();
        Symbol newFetchMarker = fetchMarker.accept(this, context);

        Reference ref = fetchStub.ref();
        Symbol newRefSymbol = ref.accept(this, context);

        if (newFetchMarker == fetchMarker && ref == newRefSymbol) {
            return fetchStub;
        }
        // Some symbol replacements want to replace references (for example, the Rename operator needs to replace References with ScopedSymbols)
        // The FetchStub `reference` part cannot and must not be turned into a ScopedSymbol, so a replacement is silently dropped here
        Reference newRef = newRefSymbol instanceof Reference ? ((Reference) newRefSymbol) : ref;
        FetchMarker newMarker = newFetchMarker instanceof FetchMarker ? ((FetchMarker) newFetchMarker) : fetchMarker;
        return new FetchStub(newMarker, newRef);
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference ref, C context) {
        return visitReference(ref, context);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, C context) {
        return symbol;
    }
}

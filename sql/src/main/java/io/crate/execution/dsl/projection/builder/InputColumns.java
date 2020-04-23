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

package io.crate.execution.dsl.projection.builder;

import com.google.common.base.MoreObjects;
import io.crate.expression.scalar.SubscriptObjectFunction;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.FetchMarker;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.FetchStub;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;

import static io.crate.common.collections.Lists2.mapTail;
import static io.crate.expression.symbol.Symbols.lookupValueByColumn;

/**
 * Provides functions to create {@link InputColumn}s
 */
@Singleton
public final class InputColumns extends DefaultTraversalSymbolVisitor<InputColumns.SourceSymbols, Symbol> {

    private static final InputColumns INSTANCE = new InputColumns();

    /**
     * Represents the "source" symbols to which the InputColumns will point to
     */
    public static class SourceSymbols {

        private final HashMap<Symbol, InputColumn> inputs;
        final IdentityHashMap<Symbol, InputColumn> nonDeterministicFunctions;

        public SourceSymbols(Collection<? extends Symbol> inputs) {
            this.inputs = new HashMap<>(inputs.size());

            // non deterministic functions would override each other in a normal hashmap
            // as they compare equal but shouldn't be treated that way here.
            // we want them to have their own Input each
            this.nonDeterministicFunctions = new IdentityHashMap<>(inputs.size());

            int i = 0;
            for (Symbol input : inputs) {
                // only non-literals should be replaced with input columns.
                // otherwise {@link io.crate.metadata.Scalar#compile} won't do anything which
                // results in poor performance of some scalar implementations
                add(i, input);

                /* SELECT count(*), x AS xx, x GROUP by 2
                 * GROUP operator would outputs: [x AS xx, count(*)]
                 * Eval wouldn't find `x`
                 */
                if (input instanceof AliasSymbol) {
                    add(i, ((AliasSymbol) input).symbol());
                }
                i++;
            }
        }

        public void add(int i, Symbol input) {
            SymbolType symbolType = input.symbolType();
            if (!symbolType.isValueSymbol()) {
                DataType<?> valueType = input.valueType();
                if ((symbolType == SymbolType.FUNCTION || symbolType == SymbolType.WINDOW_FUNCTION)
                    && !((Function) input).info().isDeterministic()) {
                    nonDeterministicFunctions.put(input, new InputColumn(i, valueType));
                } else {
                    this.inputs.put(input, new InputColumn(i, valueType));
                }
            }
        }

        public InputColumn getICForSource(Symbol source) {
            InputColumn inputColumn = inputs.get(source);
            if (inputColumn == null) {
                throw new IllegalArgumentException("source " + source + " isn't present in the source symbols: " + inputs.keySet());
            }
            return inputColumn;
        }

        @Override
        public String toString() {
            return "SourceSymbols{" +
                   "inputs=" + inputs +
                   ", nonDeterministicFunctions=" + nonDeterministicFunctions +
                   '}';
        }
    }

    /**
     * Return a symbol where each element from {@code symbolTree} that occurs in {@code inputSymbols}
     * is replaced with a {@link InputColumn} pointing to the position in {@code inputSymbols}
     *
     * <p>
     * The returned instance may be the same if no elements of {@code symbolTree} where part of {@code inputSymbols}
     * </p>
     */
    public static Symbol create(Symbol symbolTree, Collection<? extends Symbol> inputSymbols) {
        return create(symbolTree, new SourceSymbols(inputSymbols));
    }

    /**
     * Same as {@link #create(Symbol, Collection)} but allows to re-use {@link SourceSymbols}
     */
    public static Symbol create(Symbol symbolTree, SourceSymbols sourceSymbols) {
        return symbolTree.accept(INSTANCE, sourceSymbols);
    }

    /**
     * Same as {@link #create(Symbol, Collection)}, but works for multiple symbols and allows re-using
     * a {@link SourceSymbols} class.
     * <p>
     *     If {@code symbols} and the inputSymbols of the Context class are the same,
     *     it's better to use {@link InputColumn#mapToInputColumns(Collection)} to create a 1:1 InputColumn mapping.
     * </p>
     */
    public static List<Symbol> create(Collection<? extends Symbol> symbols, SourceSymbols sourceSymbols) {
        List<Symbol> result = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            result.add(symbol.accept(INSTANCE, sourceSymbols));
        }
        return result;
    }

    @Override
    public Symbol visitFunction(Function symbol, final SourceSymbols sourceSymbols) {
        Symbol replacement = getFunctionReplacementOrNull(symbol, sourceSymbols);
        if (replacement != null) {
            return replacement;
        }
        ArrayList<Symbol> replacedFunctionArgs = getProcessedArgs(symbol.arguments(), sourceSymbols);
        return new Function(symbol.info(), symbol.signature(), replacedFunctionArgs);
    }

    @Nullable
    private static Symbol getFunctionReplacementOrNull(Function symbol, SourceSymbols sourceSymbols) {
        if (symbol.info().isDeterministic()) {
            return sourceSymbols.inputs.get(symbol);
        } else {
            return sourceSymbols.nonDeterministicFunctions.get(symbol);
        }
    }

    private ArrayList<Symbol> getProcessedArgs(List<Symbol> arguments, SourceSymbols sourceSymbols) {
        ArrayList<Symbol> args = new ArrayList<>(arguments.size());
        for (Symbol arg : arguments) {
            args.add(arg.accept(this, sourceSymbols));
        }
        return args;
    }

    @Override
    public Symbol visitWindowFunction(WindowFunction windowFunction, SourceSymbols sourceSymbols) {
        Symbol replacement = getFunctionReplacementOrNull(windowFunction, sourceSymbols);
        if (replacement != null) {
            return replacement;
        }
        ArrayList<Symbol> replacedFunctionArgs = getProcessedArgs(windowFunction.arguments(), sourceSymbols);
        Symbol filterWithReplacedArgs;
        Symbol filter = windowFunction.filter();
        if (filter != null) {
            filterWithReplacedArgs = filter.accept(this, sourceSymbols);
        } else {
            filterWithReplacedArgs = null;
        }
        return new WindowFunction(
            windowFunction.info(),
            windowFunction.signature(),
            replacedFunctionArgs,
            filterWithReplacedArgs,
            windowFunction.windowDefinition()
        );
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, SourceSymbols sourceSymbols) {
        InputColumn inputColumn = sourceSymbols.inputs.get(symbol);
        if (inputColumn == null) {
            throw new IllegalArgumentException("Couldn't find " + symbol + " in " + sourceSymbols);
        }
        return inputColumn;
    }

    @Override
    public Symbol visitAlias(AliasSymbol aliasSymbol, SourceSymbols sourceSymbols) {
        InputColumn inputColumn = sourceSymbols.inputs.get(aliasSymbol);
        if (inputColumn == null) {
            Symbol column = aliasSymbol.symbol().accept(this, sourceSymbols);
            if (column == null) {
                throw new IllegalArgumentException("Couldn't find " + aliasSymbol + " in " + sourceSymbols);
            }
            return column;
        }
        return inputColumn;
    }

    @Override
    public Symbol visitLiteral(Literal symbol, SourceSymbols context) {
        return symbol;
    }

    @Override
    public Symbol visitReference(Reference ref, SourceSymbols sourceSymbols) {
        if (ref instanceof GeneratedReference) {
            return MoreObjects.firstNonNull(
                sourceSymbols.inputs.get(ref),
                (((GeneratedReference) ref).generatedExpression().accept(this, sourceSymbols)));
        }
        InputColumn inputColumn = sourceSymbols.inputs.get(ref);
        if (inputColumn == null) {
            Symbol subscriptOnRoot = tryCreateSubscriptOnRoot(ref, ref.column(), sourceSymbols.inputs);
            if (subscriptOnRoot == null) {
                return ref;
            }
            return subscriptOnRoot;
        }
        return inputColumn;
    }


    @Override
    public Symbol visitField(ScopedSymbol field, SourceSymbols sourceSymbols) {
        InputColumn inputColumn = sourceSymbols.inputs.get(field);
        if (inputColumn == null) {
            Symbol subscriptOnRoot = tryCreateSubscriptOnRoot(field, field.column(), sourceSymbols.inputs);
            if (subscriptOnRoot == null) {
                throw new IllegalArgumentException("Couldn't find " + field + " in " + sourceSymbols);
            } else {
                return subscriptOnRoot;
            }
        }
        return inputColumn;
    }

    @Override
    public Symbol visitFetchMarker(FetchMarker fetchMarker, SourceSymbols sourceSymbols) {
        InputColumn inputColumn = sourceSymbols.inputs.get(fetchMarker);
        if (inputColumn == null) {
            return fetchMarker.fetchId().accept(this, sourceSymbols);
        }
        return inputColumn;
    }

    @Override
    public Symbol visitFetchStub(FetchStub fetchStub, SourceSymbols sourceSymbols) {
        FetchMarker fetchMarker = fetchStub.fetchMarker();
        InputColumn fetchId = sourceSymbols.inputs.get(fetchMarker);
        if (fetchId == null) {
            throw new IllegalArgumentException("Could not find fetchMarker " + fetchMarker + " in sources: " + sourceSymbols);
        }
        return new FetchReference(fetchId, fetchStub.ref());
    }

    @Nullable
    private static Symbol tryCreateSubscriptOnRoot(Symbol symbol, ColumnIdent column, HashMap<Symbol, InputColumn> inputs) {
        if (column.isTopLevel()) {
            return null;
        }
        ColumnIdent root = column.getRoot();
        InputColumn rootIC = lookupValueByColumn(inputs, root);
        if (rootIC == null) {
            return symbol;
        }

        DataType<?> returnType = symbol.valueType();
        List<String> path = column.path();

        List<Symbol> arguments = mapTail(rootIC, path, Literal::of);
        List<DataType> argumentTypes = Symbols.typeView(arguments);

        return new Function(
            new FunctionInfo(new FunctionIdent(SubscriptObjectFunction.NAME, argumentTypes), returnType),
            arguments
        );
    }

    @Override
    public Symbol visitFetchReference(FetchReference fetchReference, SourceSymbols sourceSymbols) {
        throw new AssertionError("FetchReference symbols must not be visited with " + getClass().getSimpleName());
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, SourceSymbols context) {
        throw new UnsupportedOperationException(
            "A match predicate can only be evaluated if it can be linked to a single relation." +
            "Using constructs like `match(r1.c) OR match(r2.c)` is not supported.");
    }

    @Override
    public Symbol visitAggregation(Aggregation symbol, SourceSymbols sourceSymbols) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }
}

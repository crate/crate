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
import com.google.common.collect.ImmutableList;
import io.crate.expression.scalar.SubscriptObjectFunction;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;

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
                SymbolType symbolType = input.symbolType();
                if (!symbolType.isValueSymbol()) {
                    DataType valueType = input.valueType();
                    if ((symbolType == SymbolType.FUNCTION || symbolType == SymbolType.WINDOW_FUNCTION)
                        && !((Function) input).info().isDeterministic()) {
                        nonDeterministicFunctions.put(input, new InputColumn(i, valueType));
                    } else {
                        this.inputs.put(input, new InputColumn(i, valueType));
                    }
                }
                i++;
            }
        }

        public InputColumn getICForSource(Symbol source) {
            InputColumn inputColumn = inputs.get(source);
            if (inputColumn == null) {
                throw new IllegalArgumentException("source " + source + " isn't present in the source symbols: " + inputs.keySet());
            }
            return inputColumn;
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
        return INSTANCE.process(symbolTree, sourceSymbols);
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
            result.add(INSTANCE.process(symbol, sourceSymbols));
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
        return new Function(symbol.info(), replacedFunctionArgs);
    }

    @Nullable
    private Symbol getFunctionReplacementOrNull(Function symbol, SourceSymbols sourceSymbols) {
        Symbol replacement;
        if (symbol.info().isDeterministic()) {
            replacement = sourceSymbols.inputs.get(symbol);
        } else {
            replacement = sourceSymbols.nonDeterministicFunctions.get(symbol);
        }

        return replacement;
    }

    private ArrayList<Symbol> getProcessedArgs(List<Symbol> arguments, SourceSymbols sourceSymbols) {
        ArrayList<Symbol> args = new ArrayList<>(arguments.size());
        for (Symbol arg : arguments) {
            args.add(process(arg, sourceSymbols));
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
        return new WindowFunction(windowFunction.info(), replacedFunctionArgs, windowFunction.windowDefinition());
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, SourceSymbols sourceSymbols) {
        return MoreObjects.firstNonNull(sourceSymbols.inputs.get(symbol), symbol);
    }

    @Override
    public Symbol visitReference(Reference ref, SourceSymbols sourceSymbols) {
        if (ref instanceof GeneratedReference) {
            return MoreObjects.firstNonNull(
                sourceSymbols.inputs.get(ref),
                visitSymbol(((GeneratedReference) ref).generatedExpression(), sourceSymbols));
        }
        InputColumn inputColumn = sourceSymbols.inputs.get(ref);
        if (inputColumn == null) {
            Symbol subscriptOnRoot = tryCreateSubscriptOnRoot(ref, sourceSymbols.inputs);
            return subscriptOnRoot == null ? ref : subscriptOnRoot;
        }
        return visitSymbol(ref, sourceSymbols);
    }

    @Nullable
    private static Symbol tryCreateSubscriptOnRoot(Reference ref, HashMap<Symbol, InputColumn> inputs) {
        if (ref.column().isTopLevel()) {
            return null;
        }
        ColumnIdent root = ref.column().getRoot();
        InputColumn rootIC = lookupValueByColumn(inputs, root);
        if (rootIC == null) {
            return ref;
        }
        Symbol subscript = rootIC;
        List<String> path = ref.column().path();
        for (int i = 0; i < path.size(); i++) {
            boolean lastPart = i + 1 == path.size();
            DataType returnType = lastPart ? ref.valueType() : DataTypes.OBJECT;
            subscript = new Function(
                new FunctionInfo(new FunctionIdent(SubscriptObjectFunction.NAME, ImmutableList.of(DataTypes.OBJECT, DataTypes.STRING)), returnType),
                ImmutableList.of(subscript, Literal.of(path.get(i)))
            );
        }
        return subscript;
    }

    @Override
    public Symbol visitFetchReference(FetchReference fetchReference, SourceSymbols sourceSymbols) {
        return fetchReference;
    }

    @Override
    public Symbol visitAggregation(Aggregation symbol, SourceSymbols sourceSymbols) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }
}

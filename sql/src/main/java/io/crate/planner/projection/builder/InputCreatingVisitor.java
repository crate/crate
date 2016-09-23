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

package io.crate.planner.projection.builder;

import com.google.common.base.MoreObjects;
import io.crate.analyze.symbol.*;
import io.crate.types.DataType;
import org.elasticsearch.common.inject.Singleton;

import java.util.*;

@Singleton
public class InputCreatingVisitor extends DefaultTraversalSymbolVisitor<InputCreatingVisitor.Context, Symbol> {

    public static final InputCreatingVisitor INSTANCE = new InputCreatingVisitor();

    public static class Context {

        final HashMap<Symbol, InputColumn> inputs;
        final IdentityHashMap<Symbol, InputColumn> nonDeterministicFunctions;

        public Context(Collection<? extends Symbol> inputs) {
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
                if (!input.symbolType().isValueSymbol()) {
                    DataType valueType = input.valueType();
                    if (input.symbolType() == SymbolType.FUNCTION && !((Function) input).info().isDeterministic()) {
                        nonDeterministicFunctions.put(input, new InputColumn(i, valueType));
                    } else {
                        this.inputs.put(input, new InputColumn(i, valueType));
                    }
                }
                i++;
            }

        }
    }


    public List<Symbol> process(Collection<? extends Symbol> symbols, Context context) {
        List<Symbol> result = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            result.add(process(symbol, context));
        }
        return result;
    }

    @Override
    public Symbol visitFunction(Function symbol, final Context context) {
        Symbol replacement;
        if (symbol.info().isDeterministic()) {
            replacement = context.inputs.get(symbol);
        } else {
            replacement = context.nonDeterministicFunctions.get(symbol);
        }

        if (replacement != null) {
            return replacement;
        }
        ArrayList<Symbol> args = new ArrayList<>(symbol.arguments().size());
        for (Symbol arg : symbol.arguments()) {
            args.add(process(arg, context));
        }
        return new Function(symbol.info(), args);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Context context) {
        return MoreObjects.firstNonNull(context.inputs.get(symbol), symbol);
    }

    @Override
    public Symbol visitAggregation(Aggregation symbol, Context context) {
        throw new AssertionError("Aggregation Symbols must not be visited with " +
                                 getClass().getCanonicalName());
    }

}

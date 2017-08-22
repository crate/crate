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

package io.crate.analyze.symbol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Base class which can be used to create a visitor that has to replace functions.
 *
 * @param <C> context class for the visitor.
 */
public abstract class FunctionCopyVisitor<C> extends SymbolVisitor<C, Symbol> {

    /**
     * Traverses the functions arguments using {@link #process(Symbol, Object)}.
     * If any process call returns a different instance then a new Function instance is returned.
     */
    protected Function processAndMaybeCopy(Function func, C context) {
        List<Symbol> args = func.arguments();
        switch (args.size()) {
            // specialized functions to avoid allocations for common cases
            case 0:
                return func;

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
        List<Symbol> newArgs = new ArrayList<>(args.size());
        boolean changed = false;
        for (Symbol arg : args) {
            Symbol newArg = requireNonNull(process(arg, context), "function arguments must never be NULL");
            changed |= arg != newArg;
            newArgs.add(newArg);
        }
        if (changed) {
            return new Function(func.info(), newArgs);
        }
        return func;
    }

    private Function twoArgs(Function func, C context) {
        assert func.arguments().size() == 2 : "size of arguments must be two";
        Symbol arg1 = func.arguments().get(0);
        Symbol newArg1 = requireNonNull(process(arg1, context), "function arguments must never be NULL");

        Symbol arg2 = func.arguments().get(1);
        Symbol newArg2 = requireNonNull(process(arg2, context), "function arguments must never be NULL");

        if (arg1 == newArg1 && arg2 == newArg2) {
            return func;
        }
        return new Function(func.info(), Arrays.asList(newArg1, newArg2));
    }

    private Function oneArg(Function func, C context) {
        assert func.arguments().size() == 1 : "size of arguments must be one";
        Symbol arg = func.arguments().get(0);
        Symbol newArg = requireNonNull(process(arg, context), "function arguments must never be NULL");

        if (arg == newArg) {
            return func;
        }
        // Currently function arguments need to be mutable; once this is no longer the case use Collections.singletonList here
        return new Function(func.info(), Arrays.asList(newArg));
    }

    @Override
    public Symbol visitFunction(Function func, C context) {
        return processAndMaybeCopy(func, context);
    }

    @Override
    public Symbol visitDynamicReference(DynamicReference ref, C context) {
        return visitReference(ref, context);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, C context) {
        return symbol;
    }

    public List<Symbol> process(Collection<? extends Symbol> symbols, C context) {
        ArrayList<Symbol> copy = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            copy.add(process(symbol, context));
        }
        return copy;
    }
}

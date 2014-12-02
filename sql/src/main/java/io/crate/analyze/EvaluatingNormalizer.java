/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.analyze;

import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;


/**
 * the normalizer does symbol normalization and reference resolving if possible
 * <p/>
 * E.g.:
 * The query
 * <p/>
 * and(true, eq(column_ref, 'someliteral'))
 * <p/>
 * will be changed to
 * <p/>
 * eq(column_ref, 'someliteral')
 */
public class EvaluatingNormalizer extends SymbolVisitor<Void, Symbol> {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final Functions functions;
    private final RowGranularity granularity;
    private final ReferenceResolver referenceResolver;

    public EvaluatingNormalizer(
            Functions functions, RowGranularity granularity, ReferenceResolver referenceResolver) {
        this.functions = functions;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
    }

    @Override
    public Symbol visitFunction(Function function, Void context) {
        List<Symbol> newArgs = normalize(function.arguments());
        if (newArgs != function.arguments()) {
            function = new Function(function.info(), newArgs);
        }
        return normalizeFunctionSymbol(function);
    }

    @SuppressWarnings("unchecked")
    private Symbol normalizeFunctionSymbol(Function function) {
        FunctionImplementation impl = functions.get(function.info().ident());
        if (impl != null) {
            return impl.normalizeSymbol(function);
        }
        if (logger.isTraceEnabled()) {
            logger.trace(SymbolFormatter.format("No implementation found for function %s", function));
        }
        return function;
    }

    @Override
    public Symbol visitReference(Reference symbol, Void context) {
        if (symbol.info().granularity().ordinal() > granularity.ordinal()) {
            return symbol;
        }

        Input input = (Input) referenceResolver.getImplementation(symbol.info().ident());
        if (input != null) {
            return Literal.newLiteral(symbol.info().type(), input.value());
        }

        if (logger.isTraceEnabled()) {
            logger.trace(SymbolFormatter.format("Can't resolve reference %s", symbol));
        }
        return symbol;
    }

    /**
     * Same as with {@link Field#unwrap(Symbol symbol)}.
     * This is migration specific and will be removed in the future.
     */
    @Deprecated
    @Override
    public Symbol visitField(Field field, Void context) {
        Symbol target = process(field.target(), context);
        if (target.symbolType().isValueSymbol()) {
            return target;
        }
        if (!field.target().equals(target)) {
            return new Field(field.relation(), field.name(), target);
        }
        return field;
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Void context) {
        return symbol;
    }

    /**
     * Normalizes all symbols of a List. Does not return a new list if no changes occur.
     *
     * @param symbols the list to be normalized
     * @return a list with normalized symbols
     */
    public List<Symbol> normalize(List<Symbol> symbols) {
        if (symbols.size() > 0) {
            boolean changed = false;
            Symbol[] newArgs = new Symbol[symbols.size()];
            int i = 0;
            for (Symbol symbol : symbols) {
                Symbol newArg = normalize(symbol);
                changed = changed || newArg != symbol;
                newArgs[i++] = newArg;
            }
            if (changed) {
                return Arrays.asList(newArgs);
            }
        }
        return symbols;
    }

    /**
     * Normalizes all symbols of a List in place
     *
     * @param symbols the list to be normalized
     */
    public void normalizeInplace(@Nullable List<Symbol> symbols) {
        if (symbols != null){
            for (int i = 0; i < symbols.size(); i++) {
                symbols.set(i, normalize(symbols.get(i)));
            }
        }
    }

    public Symbol normalize(@Nullable Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        return process(symbol, null);
    }

}

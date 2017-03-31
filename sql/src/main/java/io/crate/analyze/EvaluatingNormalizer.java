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

import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolFormatter;
import io.crate.metadata.*;
import io.crate.data.Input;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.scalar.arithmetic.MapFunction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * the normalizer does symbol normalization and reference resolving if possible
 * <p>
 * E.g.:
 * The query
 * </p>
 * <p>
 * and(true, eq(column_ref, 'someliteral'))
 * </p>
 * <p>
 * will be changed to
 * </p>
 * <p>
 * eq(column_ref, 'someliteral')
 * </p>
 */
public class EvaluatingNormalizer {

    private static final ESLogger logger = Loggers.getLogger(EvaluatingNormalizer.class);
    private final Functions functions;
    private final RowGranularity granularity;
    private final ReferenceResolver<? extends Input<?>> referenceResolver;
    private final FieldResolver fieldResolver;
    private final BaseVisitor visitor;

    public static EvaluatingNormalizer functionOnlyNormalizer(Functions functions, ReplaceMode replaceMode) {
        return new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, replaceMode, null, null);
    }

    /**
     * @param functions         function resolver
     * @param granularity       the maximum row granularity the normalizer should try to normalize
     * @param replaceMode       defines if symbols like functions can be mutated or if they have to be copied
     * @param referenceResolver reference resolver which is used to resolve paths
     * @param fieldResolver     optional field resolver to resolve fields
     */
    public EvaluatingNormalizer(Functions functions,
                                RowGranularity granularity,
                                ReplaceMode replaceMode,
                                @Nullable ReferenceResolver<? extends Input<?>> referenceResolver,
                                @Nullable FieldResolver fieldResolver) {
        this.functions = functions;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
        this.fieldResolver = fieldResolver;
        if (replaceMode == ReplaceMode.MUTATE) {
            this.visitor = new InPlaceVisitor();
        } else {
            this.visitor = new CopyingVisitor();
        }
    }

    private static class Context {

        @Nullable
        private final TransactionContext transactionContext;

        public Context(@Nullable TransactionContext transactionContext) {
            this.transactionContext = transactionContext;
        }
    }

    private abstract class BaseVisitor extends SymbolVisitor<Context, Symbol> {
        @Override
        public Symbol visitField(Field field, Context context) {
            if (fieldResolver != null) {
                Symbol resolved = fieldResolver.resolveField(field);
                if (resolved != null) {
                    return resolved;
                }
            }
            return field;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Context context) {
            if (fieldResolver != null) {
                // Once the fields can be resolved, rewrite matchPredicate to function
                Map<Field, Symbol> fieldBoostMap = matchPredicate.identBoostMap();

                List<Symbol> columnBoostMapArgs = new ArrayList<>(fieldBoostMap.size() * 2);
                for (Map.Entry<Field, Symbol> entry : fieldBoostMap.entrySet()) {
                    Symbol resolved = process(entry.getKey(), null);
                    if (resolved instanceof Reference) {
                        columnBoostMapArgs.add(Literal.of(((Reference) resolved).ident().columnIdent().fqn()));
                        columnBoostMapArgs.add(entry.getValue());
                    } else {
                        return matchPredicate;
                    }
                }

                Function function = new Function(
                    io.crate.operation.predicate.MatchPredicate.INFO,
                    Arrays.asList(
                        new Function(MapFunction.createInfo(Symbols.extractTypes(columnBoostMapArgs)), columnBoostMapArgs),
                        matchPredicate.queryTerm(),
                        Literal.of(matchPredicate.matchType()),
                        matchPredicate.options()
                    ));
                return process(function, context);
            }
            return matchPredicate;
        }


        @SuppressWarnings("unchecked")
        Symbol normalizeFunctionSymbol(Function function, Context context) {
            FunctionIdent ident = function.info().ident();
            FunctionImplementation impl = functions.get(ident.schema(), ident.name(), ident.argumentTypes());
            if (impl != null) {
                return impl.normalizeSymbol(function, context.transactionContext);
            }
            if (logger.isTraceEnabled()) {
                logger.trace(SymbolFormatter.format("No implementation found for function %s", function));
            }
            return function;
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            if (referenceResolver == null || symbol.granularity().ordinal() > granularity.ordinal()) {
                return symbol;
            }

            Input input = referenceResolver.getImplementation(symbol);
            if (input != null) {
                return Literal.of(symbol.valueType(), input.value());
            }

            if (logger.isTraceEnabled()) {
                logger.trace(SymbolFormatter.format("Can't resolve reference %s", symbol));
            }
            return symbol;
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Context context) {
            return symbol;
        }
    }

    private class CopyingVisitor extends BaseVisitor {
        @Override
        public Symbol visitFunction(Function function, Context context) {
            List<Symbol> newArgs = normalize(function.arguments(), context);
            if (newArgs != function.arguments()) {
                function = new Function(function.info(), newArgs);
            }
            return normalizeFunctionSymbol(function, context);
        }
    }

    private class InPlaceVisitor extends BaseVisitor {
        @Override
        public Symbol visitFunction(Function function, Context context) {
            normalizeInplace(function.arguments(), context);
            return normalizeFunctionSymbol(function, context);
        }
    }

    /**
     * Normalizes all symbols of a List. Does not return a new list if no changes occur.
     *
     * @param symbols the list to be normalized
     * @return a list with normalized symbols
     */
    public List<Symbol> normalize(List<Symbol> symbols, TransactionContext transactionContext) {
        Context context = new Context(transactionContext);
        return normalize(symbols, context);
    }

    private List<Symbol> normalize(List<Symbol> symbols, Context context) {
        if (symbols.size() > 0) {
            boolean changed = false;
            Symbol[] newArgs = new Symbol[symbols.size()];
            int i = 0;
            for (Symbol symbol : symbols) {
                Symbol newArg = normalize(symbol, context);
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
    public void normalizeInplace(@Nullable List<Symbol> symbols, @Nullable TransactionContext transactionContext) {
        Context context = new Context(transactionContext);
        normalizeInplace(symbols, context);
    }

    private void normalizeInplace(@Nullable List<Symbol> symbols, Context context) {
        if (symbols != null) {
            for (int i = 0; i < symbols.size(); i++) {
                symbols.set(i, normalize(symbols.get(i), context));
            }
        }
    }

    public Symbol normalize(@Nullable Symbol symbol, @Nullable TransactionContext transactionContext) {
        return normalize(symbol, new Context(transactionContext));
    }

    private Symbol normalize(@Nullable Symbol symbol, Context context) {
        if (symbol == null) {
            return null;
        }
        return visitor.process(symbol, context);
    }

}

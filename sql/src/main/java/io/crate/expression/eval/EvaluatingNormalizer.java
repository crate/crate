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

package io.crate.expression.eval;

import io.crate.analyze.relations.FieldResolver;
import io.crate.data.Input;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * The normalizer does several things:
 *
 *  - Convert functions into a simpler form by using {@link FunctionImplementation#normalizeSymbol(Function, TransactionContext)}
 *  - Convert {@link ScopedSymbol} to {@link Reference} if {@link FieldResolver} is available.
 *  - Convert {@link MatchPredicate} to a {@link Function} if {@link FieldResolver} is available
 *  - Convert {@link Reference} into a Literal value if {@link ReferenceResolver} is available
 *    and {@link NestableInput}s can be retrieved for the Reference.
 */
public class EvaluatingNormalizer {

    private static final Logger LOGGER = LogManager.getLogger(EvaluatingNormalizer.class);
    private final Functions functions;
    private final RowGranularity granularity;
    private final ReferenceResolver<? extends Input<?>> referenceResolver;
    private final FieldResolver fieldResolver;
    private final BaseVisitor visitor;

    public static EvaluatingNormalizer functionOnlyNormalizer(Functions functions) {
        return new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, null, null);
    }

    /**
     * @param functions         function resolver
     * @param granularity       the maximum row granularity the normalizer should try to normalize
     * @param referenceResolver reference resolver which is used to resolve paths
     * @param fieldResolver     optional field resolver to resolve fields
     */
    public EvaluatingNormalizer(Functions functions,
                                RowGranularity granularity,
                                @Nullable ReferenceResolver<? extends Input<?>> referenceResolver,
                                @Nullable FieldResolver fieldResolver) {
        this.functions = functions;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
        this.fieldResolver = fieldResolver;
        this.visitor = new BaseVisitor();
    }

    private class BaseVisitor extends FunctionCopyVisitor<TransactionContext> {
        @Override
        public Symbol visitField(ScopedSymbol field, TransactionContext context) {
            if (fieldResolver != null) {
                Symbol resolved = fieldResolver.resolveField(field);
                if (resolved != null) {
                    return resolved;
                }
            }
            return field;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, TransactionContext context) {
            if (fieldResolver != null) {
                // Once the fields can be resolved, rewrite matchPredicate to function
                Map<Symbol, Symbol> fieldBoostMap = matchPredicate.identBoostMap();

                List<Symbol> columnBoostMapArgs = new ArrayList<>(fieldBoostMap.size() * 2);
                for (Map.Entry<Symbol, Symbol> entry : fieldBoostMap.entrySet()) {
                    Symbol resolved = entry.getKey().accept(this, null);
                    if (resolved instanceof Reference) {
                        columnBoostMapArgs.add(Literal.of(((Reference) resolved).column().fqn()));
                        columnBoostMapArgs.add(entry.getValue());
                    } else {
                        return matchPredicate;
                    }
                }

                Function function = new Function(
                    io.crate.expression.predicate.MatchPredicate.INFO,
                    Arrays.asList(
                        new Function(
                            MapFunction.createInfo(Symbols.typeView(columnBoostMapArgs)),
                            MapFunction.SIGNATURE,
                            columnBoostMapArgs,
                            null
                        ),
                        matchPredicate.queryTerm(),
                        Literal.of(matchPredicate.matchType()),
                        matchPredicate.options()
                    ));
                return ((Symbol) function).accept(this, context);
            }
            return matchPredicate;
        }

        @Override
        public Symbol visitReference(Reference symbol, TransactionContext context) {
            if (referenceResolver == null || symbol.granularity().ordinal() > granularity.ordinal()) {
                return symbol;
            }

            Input<?> input = referenceResolver.getImplementation(symbol);
            if (input != null) {
                return Literal.ofUnchecked(symbol.valueType(), input.value());
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(Symbols.format("Can't resolve reference %s", symbol));
            }
            return symbol;
        }

        @Override
        public Symbol visitFunction(Function function, TransactionContext context) {
            return normalizeFunction(function, context);
        }

        private Symbol normalizeFunction(Function function, TransactionContext context) {
            function = processAndMaybeCopy(function, context);
            var ident = function.info().ident();
            var signature = function.signature();
            FunctionImplementation implementation;
            if (signature == null) {
                implementation = functions.getQualified(ident);
            } else {
                implementation = functions.getQualified(signature, ident.argumentTypes());
            }
            assert implementation != null : "Function implementation not found using full qualified lookup: " + function;
            return implementation.normalizeSymbol(function, context);
        }

        @Override
        public Symbol visitWindowFunction(WindowFunction function, TransactionContext context) {
            Function normalizedFunction = (Function) normalizeFunction(function, context);
            return new WindowFunction(
                normalizedFunction.info(),
                normalizedFunction.signature(),
                normalizedFunction.arguments(),
                normalizedFunction.filter(),
                function.windowDefinition().map(s -> s.accept(this, context))
            );
        }
    }

    public Symbol normalize(@Nullable Symbol symbol, @Nullable TransactionContext txnCtx) {
        if (symbol == null) {
            return null;
        }
        return symbol.accept(visitor, txnCtx);
    }
}

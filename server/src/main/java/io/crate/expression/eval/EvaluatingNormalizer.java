/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.eval;

import static io.crate.expression.predicate.MatchPredicate.TEXT_MATCH;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.FieldResolver;
import io.crate.data.Input;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.scalar.arithmetic.MapFunction;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;


/**
 * The normalizer does several things:
 *
 *  - Convert functions into a simpler form by using {@link FunctionImplementation#normalizeSymbol(Function, TransactionContext, NodeContext)}
 *  - Convert {@link ScopedSymbol} to {@link Reference} if {@link FieldResolver} is available.
 *  - Convert {@link MatchPredicate} to a {@link Function} if {@link FieldResolver} is available
 *  - Convert {@link Reference} into a Literal value if {@link ReferenceResolver} is available
 *    and {@link NestableInput}s can be retrieved for the Reference.
 */
public class EvaluatingNormalizer {

    private static final Logger LOGGER = LogManager.getLogger(EvaluatingNormalizer.class);
    private final NodeContext nodeCtx;
    private final RowGranularity granularity;
    private final ReferenceResolver<? extends Input<?>> referenceResolver;
    private final FieldResolver fieldResolver;
    private final BaseVisitor visitor;
    private final Predicate<Function> onFunctionCondition;

    public static EvaluatingNormalizer functionOnlyNormalizer(NodeContext nodeCtx) {
        return new EvaluatingNormalizer(nodeCtx, RowGranularity.CLUSTER, null, null);
    }

    public static EvaluatingNormalizer functionOnlyNormalizer(NodeContext nodeCtx,
                                                              Predicate<Function> onFunctionCondition) {
        return new EvaluatingNormalizer(
            nodeCtx,
            RowGranularity.CLUSTER,
            null,
            null,
            onFunctionCondition
        );
    }

    public EvaluatingNormalizer(NodeContext nodeCtx,
                                RowGranularity granularity,
                                @Nullable ReferenceResolver<? extends Input<?>> referenceResolver,
                                @Nullable FieldResolver fieldResolver) {
        this(nodeCtx, granularity, referenceResolver, fieldResolver, function -> true);
    }

    /**
     * @param nodeCtx           function resolver
     * @param granularity       the maximum row granularity the normalizer should try to normalize
     * @param referenceResolver reference resolver which is used to resolve paths
     * @param fieldResolver     optional field resolver to resolve fields
     */
    public EvaluatingNormalizer(NodeContext nodeCtx,
                                RowGranularity granularity,
                                @Nullable ReferenceResolver<? extends Input<?>> referenceResolver,
                                @Nullable FieldResolver fieldResolver,
                                Predicate<Function> onFunctionCondition) {
        this.nodeCtx = nodeCtx;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
        this.fieldResolver = fieldResolver;
        this.visitor = new BaseVisitor();
        this.onFunctionCondition = onFunctionCondition;
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
                    if (resolved instanceof Reference ref) {
                        columnBoostMapArgs.add(Literal.of(ref.storageIdent()));
                        columnBoostMapArgs.add(entry.getValue());
                    } else {
                        return matchPredicate;
                    }
                }

                List<Symbol> arguments = List.of(
                    new Function(
                        MapFunction.SIGNATURE,
                        columnBoostMapArgs,
                        DataTypes.UNTYPED_OBJECT).accept(this, context),
                    matchPredicate.queryTerm().accept(this, context),
                    Literal.of(matchPredicate.matchType()),
                    matchPredicate.options().accept(this, context)
                );
                FunctionImplementation implementation = nodeCtx.functions().get(
                    null,
                    io.crate.expression.predicate.MatchPredicate.NAME,
                    arguments,
                    context.sessionSettings().searchPath()
                );
                // In 4.1 the match function was registered to (object, text, text, object)
                //
                // But now the default constructor for Function creates a FunctionInfo where it uses the types of the arguments
                // And the arguments here can be (object, geo_shape, text, object) for MATCH on shapes
                // For mixed cluster compatibility it is necessary to use a "incorrect" FunctionInfo
                return new Function(implementation.signature(), arguments, DataTypes.BOOLEAN) {

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        if (out.getVersion().onOrAfter(Version.V_4_2_4)) {
                            super.writeTo(out);
                        } else {
                            TEXT_MATCH.writeAsFunctionInfo(out, TEXT_MATCH.getArgumentDataTypes());
                            if (out.getVersion().onOrAfter(Version.V_4_1_0)) {
                                Symbols.nullableToStream(filter, out);
                            }
                            Symbols.toStream(arguments, out);
                            if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
                                out.writeBoolean(true);
                                signature.writeTo(out);
                                DataTypes.toStream(returnType, out);
                            }
                        }
                    }
                };
            }

            HashMap<Symbol, Symbol> fieldBoostMap = new HashMap<>(matchPredicate.identBoostMap().size());
            for (var entry : matchPredicate.identBoostMap().entrySet()) {
                fieldBoostMap.put(
                    entry.getKey().accept(this, context),
                    entry.getValue().accept(this, context)
                );
            }
            return new MatchPredicate(
                fieldBoostMap,
                matchPredicate.queryTerm().accept(this, context),
                matchPredicate.matchType(),
                matchPredicate.options().accept(this, context)
            );
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
            if (onFunctionCondition.test(function) == false) {
                return function;
            }
            function = processAndMaybeCopy(function, context);
            FunctionImplementation implementation = nodeCtx.functions().getQualified(function);
            assert implementation != null : "Function implementation not found using full qualified lookup: " + function;
            return implementation.normalizeSymbol(function, context, nodeCtx);
        }

        @Override
        public Symbol visitWindowFunction(WindowFunction function, TransactionContext context) {
            Function normalizedFunction = (Function) normalizeFunction(function, context);
            return new WindowFunction(
                normalizedFunction.signature(),
                normalizedFunction.arguments(),
                normalizedFunction.valueType(),
                normalizedFunction.filter(),
                function.windowDefinition().map(s -> s.accept(this, context)),
                function.ignoreNulls()
            );
        }

        @Override
        public Symbol visitAlias(AliasSymbol aliasSymbol, TransactionContext context) {
            return aliasSymbol.symbol().accept(this, context);
        }
    }

    public Symbol normalize(@Nullable Symbol symbol, @NotNull TransactionContext txnCtx) {
        if (symbol == null) {
            return null;
        }
        return symbol.accept(visitor, txnCtx);
    }
}

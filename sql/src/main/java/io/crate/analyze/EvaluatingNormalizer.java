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
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.operation.Input;
import io.crate.operation.reference.ReferenceResolver;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
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


    /**
     * @param functions function resolver
     * @param granularity the maximum row granularity the normalizer should try to normalize
     * @param referenceResolver reference resolver which is used to resolve paths
     * @param fieldResolver optional field resolver to resolve fields
     * @param inPlace defines if symbols like functions can be changed inplace instead of being copied when changed
     */
    public EvaluatingNormalizer(
            Functions functions, RowGranularity granularity, ReferenceResolver<? extends Input<?>> referenceResolver,
            @Nullable FieldResolver fieldResolver, boolean inPlace) {
        this.functions = functions;
        this.granularity = granularity;
        this.referenceResolver = referenceResolver;
        this.fieldResolver = fieldResolver;
        if (inPlace) {
            this.visitor = new InPlaceVisitor();
        } else {
            this.visitor = new CopyingVisitor();
        }
    }

    public EvaluatingNormalizer(
            Functions functions, RowGranularity granularity, ReferenceResolver<? extends Input<?>> referenceResolver) {
        this(functions, granularity, referenceResolver, null, false);
    }

    public EvaluatingNormalizer(AnalysisMetaData analysisMetaData, FieldResolver fieldResolver, boolean inPlace) {
        this(analysisMetaData.functions(), RowGranularity.CLUSTER,
                analysisMetaData.referenceResolver(), fieldResolver, inPlace);
    }

    private abstract class BaseVisitor extends SymbolVisitor<Void, Symbol> {
        @Override
        public Symbol visitField(Field field, Void context) {
            if (fieldResolver != null) {
                Symbol resolved = fieldResolver.resolveField(field);
                if (resolved != null) {
                    return resolved;
                }
            }
            return field;
        }

        @Override
        public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Void Context) {
            if (fieldResolver != null) {
                // Once the fields can be resolved, rewrite matchPredicate to function
                Map<Field, Double> fieldBoostMap = matchPredicate.identBoostMap();
                Map<String, Object> fqnBoostMap = new HashMap<>(fieldBoostMap.size());

                for (Map.Entry<Field, Double> entry : fieldBoostMap.entrySet()) {
                    Symbol resolved = process(entry.getKey(), null);
                    if (resolved instanceof Reference) {
                        fqnBoostMap.put(((Reference) resolved).info().ident().columnIdent().fqn(), entry.getValue());
                    } else {
                        return matchPredicate;
                    }
                }

                return new Function(
                        io.crate.operation.predicate.MatchPredicate.INFO,
                        Arrays.<Symbol>asList(
                                Literal.newLiteral(fqnBoostMap),
                                Literal.newLiteral(matchPredicate.columnType(), matchPredicate.queryTerm()),
                                Literal.newLiteral(matchPredicate.matchType()),
                                Literal.newLiteral(matchPredicate.options())));
            }
            return matchPredicate;
        }


        @SuppressWarnings("unchecked")
        protected Symbol normalizeFunctionSymbol(Function function) {
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

            Input input = referenceResolver.getImplementation(symbol.info());
            if (input != null) {
                return Literal.newLiteral(symbol.info().type(), input.value());
            }

            if (logger.isTraceEnabled()) {
                logger.trace(SymbolFormatter.format("Can't resolve reference %s", symbol));
            }
            return symbol;
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Void context) {
            return symbol;
        }
    }

    private class CopyingVisitor extends BaseVisitor {
        @Override
        public Symbol visitFunction(Function function, Void context) {
            List<Symbol> newArgs = normalize(function.arguments());
            if (newArgs != function.arguments()) {
                function = new Function(function.info(), newArgs);
            }
            return normalizeFunctionSymbol(function);
        }
    }

    private class InPlaceVisitor extends BaseVisitor {
        @Override
        public Symbol visitFunction(Function function, Void context) {
            normalizeInplace(function.arguments());
            return normalizeFunctionSymbol(function);
        }
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
        if (symbols != null) {
            for (int i = 0; i < symbols.size(); i++) {
                symbols.set(i, normalize(symbols.get(i)));
            }
        }
    }

    public Symbol normalize(@Nullable Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        return visitor.process(symbol, null);
    }

}

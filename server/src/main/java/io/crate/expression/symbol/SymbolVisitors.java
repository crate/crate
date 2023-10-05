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

package io.crate.expression.symbol;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public final class SymbolVisitors {

    private SymbolVisitors() {}

    private static final AnyPredicateVisitor ANY_VISITOR = new AnyPredicateVisitor();
    private static final ExtractAnalyzedRelationsVisitor EXTRACT_ANALYZED_RELATIONS_VISITOR =
        new ExtractAnalyzedRelationsVisitor();

    public static boolean any(Predicate<? super Symbol> predicate, List<? extends Symbol> symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            if (any(predicate, symbols.get(i))) {
                return true;
            }
        }
        return false;
    }

    public static boolean any(Predicate<? super Symbol> predicate, Iterable<? extends Symbol> symbols) {
        for (Symbol symbol: symbols) {
            if (any(predicate, symbol)) {
                return true;
            }
        }
        return false;
    }

    public static boolean any(Predicate<? super Symbol> symbolPredicate, Symbol symbol) {
        return symbol.accept(ANY_VISITOR, symbolPredicate);
    }

    /**
     * Calls the given `consumer` for all intersection points between `needle` and `haystack`.
     * For example, in:
     * <pre>
     *     needle: x > 20 AND y = 2
     *     haystack: [x, y = 2]
     * </pre>
     *
     * The `consumer` would be called for `x` and for `y = 2`
     */
    public static <T> void intersection(Symbol needle, Collection<T> haystack, Consumer<T> consumer) {
        needle.accept(new IntersectionVisitor<>(haystack, consumer), null);
    }

    public static Iterable<AnalyzedRelation> extractAnalyzedRelations(@Nullable Symbol symbol) {
        if (symbol == null) {
            return Set.of();
        }
        Set<AnalyzedRelation> relations = new HashSet<>();
        symbol.accept(EXTRACT_ANALYZED_RELATIONS_VISITOR, relations);
        return relations;
    }

    // If `haystack.contains(x)` is true, then `x` has type `T`, and the call to the consumer is safe.
    // This provides convenience for call-ees of `intersection`.
    // If `haystack` is for example `List<Function>` they can use a `Consumer<Function>` and don't loose type information.
    @SuppressWarnings({"SuspiciousMethodCalls", "unchecked"})
    private static class IntersectionVisitor<T> extends SymbolVisitor<Void, Void> {

        private final Collection<T> haystack;
        private final Consumer<T> consumer;

        public IntersectionVisitor(Collection<T> haystack, Consumer<T> consumer) {
            this.haystack = haystack;
            this.consumer = consumer;
        }

        @Override
        public Void visitFunction(Function func, Void context) {
            if (haystack.contains(func)) {
                consumer.accept((T) func);
            } else {
                for (Symbol argument : func.arguments()) {
                    callConsumerOrVisit(argument);
                }
                Symbol filter = func.filter();
                if (filter != null) {
                    callConsumerOrVisit(filter);
                }
            }
            return null;
        }

        @Override
        public Void visitWindowFunction(WindowFunction windowFunc, Void context) {
            if (haystack.contains(windowFunc)) {
                consumer.accept((T) windowFunc);
            } else {
                for (Symbol argument : windowFunc.arguments()) {
                    callConsumerOrVisit(argument);
                }
                Symbol filter = windowFunc.filter();
                if (filter != null) {
                    callConsumerOrVisit(filter);
                }
                WindowDefinition windowDefinition = windowFunc.windowDefinition();
                for (Symbol partition : windowDefinition.partitions()) {
                    callConsumerOrVisit(partition);
                }
                OrderBy orderBy = windowDefinition.orderBy();
                if (orderBy != null) {
                    for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
                        callConsumerOrVisit(orderBySymbol);
                    }
                }
                Symbol startOffsetValue = windowDefinition.windowFrameDefinition().start().value();
                if (startOffsetValue != null) {
                    callConsumerOrVisit(startOffsetValue);
                }
                Symbol endOffsetValue = windowDefinition.windowFrameDefinition().end().value();
                if (endOffsetValue != null) {
                    callConsumerOrVisit(endOffsetValue);
                }
            }
            return null;
        }

        private void callConsumerOrVisit(Symbol symbol) {
            if (haystack.contains(symbol)) {
                consumer.accept((T) symbol);
            } else {
                symbol.accept(this, null);
            }
        }

        @Override
        public Void visitAlias(AliasSymbol aliasSymbol, Void context) {
            if (haystack.contains(aliasSymbol)) {
                consumer.accept((T) aliasSymbol);
            } else {
                aliasSymbol.symbol().accept(this, context);
            }
            return null;
        }

        @Override
        public Void visitField(ScopedSymbol field, Void context) {
            if (haystack.contains(field)) {
                consumer.accept((T) field);
            } else if (!field.column().isTopLevel()) {
                // needle: `obj[x]`, haystack: [`obj`] -> `obj` is an intersection
                ColumnIdent root = field.column().getRoot();
                for (T t : haystack) {
                    if (t instanceof ScopedSymbol && ((ScopedSymbol) t).column().equals(root)) {
                        consumer.accept(t);
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitReference(Reference ref, Void context) {
            if (haystack.contains(ref)) {
                consumer.accept((T) ref);
            } else if (!ref.column().isTopLevel()) {
                // needle: `obj[x]`, haystack: [`obj`] -> `obj` is an intersection
                ColumnIdent root = ref.column().getRoot();
                for (T t : haystack) {
                    if (t instanceof Reference tRef && tRef.column().equals(root)) {
                        consumer.accept(t);
                        break;
                    } else if (ref instanceof VoidReference
                               && t instanceof ScopedSymbol tScopedSymbol
                               && tScopedSymbol.relation().equals(ref.ident().tableIdent())
                               && tScopedSymbol.column().equals(root)) {
                        consumer.accept(t);
                        break;
                    }
                }
            }
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Void context) {
            if (haystack.contains(symbol)) {
                consumer.accept((T) symbol);
            }
            return null;
        }
    }

    private static class AnyPredicateVisitor extends SymbolVisitor<Predicate<? super Symbol>, Boolean> {

        @Override
        public Boolean visitFunction(Function symbol, Predicate<? super Symbol> symbolPredicate) {
            if (symbolPredicate.test(symbol)) {
                return true;
            }
            for (Symbol arg : symbol.arguments()) {
                if (arg.accept(this, symbolPredicate)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitWindowFunction(WindowFunction symbol, Predicate<? super Symbol> context) {
            return visitFunction(symbol, context);
        }

        @Override
        public Boolean visitFetchReference(FetchReference fetchReference, Predicate<? super Symbol> symbolPredicate) {
            return symbolPredicate.test(fetchReference)
                   || fetchReference.fetchId().accept(this, symbolPredicate)
                   || fetchReference.ref().accept(this, symbolPredicate);
        }

        @Override
        public Boolean visitMatchPredicate(MatchPredicate matchPredicate, Predicate<? super Symbol> symbolPredicate) {
            if (symbolPredicate.test(matchPredicate)) {
                return true;
            }
            for (Symbol field : matchPredicate.identBoostMap().keySet()) {
                if (field.accept(this, symbolPredicate)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitAlias(AliasSymbol aliasSymbol, Predicate<? super Symbol> predicate) {
            if (predicate.test(aliasSymbol)) {
                return true;
            }
            return aliasSymbol.symbol().accept(this, predicate);
        }

        @Override
        public Boolean visitOuterColumn(OuterColumn outerColumn, Predicate<? super Symbol> predicate) {
            if (predicate.test(outerColumn)) {
                return true;
            }
            return outerColumn.symbol().accept(this, predicate);
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Predicate<? super Symbol> symbolPredicate) {
            return symbolPredicate.test(symbol);
        }

        @Override
        public Boolean visitSelectSymbol(SelectSymbol selectSymbol, Predicate<? super Symbol> predicate) {
            return predicate.test(selectSymbol);
        }
    }

    private static class ExtractAnalyzedRelationsVisitor
        extends DefaultTraversalSymbolVisitor<Set<AnalyzedRelation>, Void> {

        @Override
        public Void visitSelectSymbol(SelectSymbol selectSymbol, Set<AnalyzedRelation> context) {
            context.add(selectSymbol.relation());
            return null;
        }
    }
}

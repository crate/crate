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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public final class SymbolVisitors {

    private SymbolVisitors() {}

    public static boolean any(Predicate<? super Symbol> predicate, List<? extends Symbol> symbols) {
        for (int i = 0; i < symbols.size(); i++) {
            Symbol symbol = symbols.get(i);
            if (symbol.any(predicate)) {
                return true;
            }
        }
        return false;
    }

    public static boolean any(Predicate<? super Symbol> predicate, Iterable<? extends Symbol> symbols) {
        for (Symbol symbol: symbols) {
            if (symbol.any(predicate)) {
                return true;
            }
        }
        return false;
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
            } else if (!field.column().isRoot()) {
                // needle: `obj[x]`, haystack: [`obj`] -> `obj` is an intersection
                ColumnIdent root = field.column().getRoot();
                for (T t : haystack) {
                    if (t instanceof ScopedSymbol scopedSymbol && scopedSymbol.column().equals(root)) {
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
            } else if (!ref.column().isRoot()) {
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
}

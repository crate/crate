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

package io.crate.analyze.relations;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.Set;

class FieldCollectingVisitor extends DefaultTraversalSymbolVisitor<FieldCollectingVisitor.Context, Void> {

    private static final Supplier<Set<Symbol>> FIELD_SET_SUPPLIER = new Supplier<Set<Symbol>>() {
        @Override
        public Set<Symbol> get() {
            // a LinkedHashSet is required for deterministic output order
            return new LinkedHashSet<>();
        }
    };

    public static class Context {
        final Multimap<AnalyzedRelation, Symbol> fields;
        final Predicate<Symbol> skipIf;

        public Context(int expectedNumRelations, Predicate<Symbol> skipIf){
            fields = Multimaps.newSetMultimap(
                    new IdentityHashMap<AnalyzedRelation, Collection<Symbol>>(expectedNumRelations),
                    FIELD_SET_SUPPLIER);
            this.skipIf = skipIf;
        }

        public Context(int expectedNumRelations) {
            this(expectedNumRelations, Predicates.<Symbol>alwaysFalse());
        }
    }

    private FieldCollectingVisitor() {
    }

    public static final FieldCollectingVisitor INSTANCE = new FieldCollectingVisitor();

    @Override
    public Void process(Symbol symbol, @Nullable Context context) {
        assert context != null;
        if (!context.skipIf.apply(symbol)) {
            super.process(symbol, context);
        }
        return null;
    }

    public void process(Iterable<? extends Symbol> symbols, FieldCollectingVisitor.Context context) {
        for (Symbol symbol : symbols) {
            process(symbol, context);
        }
    }

    @Override
    public Void visitField(Field field, FieldCollectingVisitor.Context context) {
        context.fields.put(field.relation(), field);
        return null;
    }
}

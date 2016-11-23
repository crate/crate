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

package io.crate.analyze.fetch;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.doc.DocSysColumns;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FetchFieldExtractor {

    public static Set<Field> process(List<Symbol> symbols, Multimap<AnalyzedRelation, Symbol> subOutputs) {
        HashSet<Field> canBeFetched = new HashSet<>();
        Context ctx = new Context(subOutputs, canBeFetched);
        for (Symbol symbol : symbols) {
            FieldVisitor.INSTANCE.process(symbol, ctx);
        }
        return canBeFetched;
    }

    private static class Context {
        private final Multimap<AnalyzedRelation, Symbol> outputs;
        private final Collection<Field> canBeFetched;
        private final Collection<Symbol> skipSymbols;

        private Context(Multimap<AnalyzedRelation, Symbol> outputs, Collection<Field> canBeFetched) {
            this.canBeFetched = canBeFetched;
            this.skipSymbols = outputs.values();
            this.outputs = outputs;
        }
    }

    private static class FieldVisitor extends DefaultTraversalSymbolVisitor<Context, Boolean> {

        private static final FieldVisitor INSTANCE = new FieldVisitor();

        @Override
        protected Boolean visitSymbol(Symbol symbol, Context context) {
            return !context.skipSymbols.contains(symbol);
        }

        @Override
        public Boolean visitField(Field field, Context context) {
            if (context.skipSymbols.contains(field)) {
                return false;
            }
            if (IsFetchableVisitor.isFetchable(field)) {
                context.canBeFetched.add(field);
                return true;
            } else {
                // if it is not fetchable, the field needs to be in the outputs of the according relation
                context.outputs.put(field.relation(), field);
                return false;
            }
        }
    }

    private static class IsFetchableVisitor extends AnalyzedRelationVisitor<Field, Boolean> {

        private static final IsFetchableVisitor INSTANCE = new IsFetchableVisitor();
        private static final Set<Path> NOT_FETCHABLE = ImmutableSet.<Path>of(DocSysColumns.SCORE, DocSysColumns.FETCHID);

        public static Boolean isFetchable(Field field) {
            return INSTANCE.process(field.relation(), field);
        }

        @Override
        public Boolean visitDocTableRelation(DocTableRelation relation, Field field) {
            return !NOT_FETCHABLE.contains(field.path());
        }

        @Override
        protected Boolean visitAnalyzedRelation(AnalyzedRelation relation, Field context) {
            return false;
        }
    }

}

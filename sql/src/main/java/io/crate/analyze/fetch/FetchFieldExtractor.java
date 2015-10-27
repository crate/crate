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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.doc.DocSysColumns;

import java.util.Collection;

public class FetchFieldExtractor {

    public static void extract(Iterable<? extends Symbol> symbols, Collection<Symbol> fields){
        for (Symbol symbol : symbols) {
            FieldVisitor.INSTANCE.process(symbol, fields);
        }
    }

    private static class FieldVisitor extends DefaultTraversalSymbolVisitor<Collection<Symbol>, Void>{

        private static final FieldVisitor INSTANCE = new FieldVisitor();


        @Override
        public Void visitField(Field field, Collection<Symbol> context) {
            if (!IsFetchableVisitor.isFetchable(field)){
                context.add(field);
            }
            return null;
        }
    }

    private static class IsFetchableVisitor extends AnalyzedRelationVisitor<Field, Boolean> {

        private static final IsFetchableVisitor INSTANCE = new IsFetchableVisitor();

        public static Boolean isFetchable(Field field){
            return INSTANCE.process(field.relation(), field);
        }

        @Override
        public Boolean visitDocTableRelation(DocTableRelation relation, Field field) {
            return !field.path().equals(DocSysColumns.SCORE);
        }

        @Override
        protected Boolean visitAnalyzedRelation(AnalyzedRelation relation, Field context) {
            return false;
        }
    }

}

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

import com.google.common.collect.Multimap;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;

final class FieldCollectingVisitor extends DefaultTraversalSymbolVisitor<Multimap<AnalyzedRelation, ? super Field>, Void> {

    private static final FieldCollectingVisitor INSTANCE = new FieldCollectingVisitor();

    private FieldCollectingVisitor() {
    }

    public static void collectFields(Symbol symbol,
                                     Multimap<AnalyzedRelation, ? super Field> fieldsByRelation) {
        INSTANCE.process(symbol, fieldsByRelation);
    }

    public static void collectFields(Iterable<? extends Symbol> symbols,
                                     Multimap<AnalyzedRelation, ? super Field> fieldsByRelation) {
        for (Symbol symbol : symbols) {
            INSTANCE.process(symbol, fieldsByRelation);
        }
    }

    @Override
    public Void visitField(Field field, Multimap<AnalyzedRelation, ? super Field> fieldsByRelation) {
        fieldsByRelation.put(field.relation(), field);
        return null;
    }
}

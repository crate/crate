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

package io.crate.analyze;

import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class TableIdentsExtractor {

    private static final TableIdentRelationVisitor RELATION_TABLE_IDENT_EXTRACTOR = new TableIdentRelationVisitor();

    /**
     * Extracts all table idents from all given symbols if possible (some symbols don't provide any table ident info)
     */
    public static Collection<RelationName> extract(Iterable<? extends Symbol> symbols) {
        HashSet<RelationName> relationNames = new HashSet<>();
        for (Symbol symbol : symbols) {
            relationNames.addAll(extract(symbol));
        }
        return relationNames;
    }

    /**
     * Extracts all table idents from the given symbol if possible (some symbols don't provide any table ident info)
     */
    public static Set<RelationName> extract(Symbol symbol) {
        HashSet<RelationName> relationNames = new HashSet<>();
        symbol.any(node -> {
            switch (node) {
                case ScopedSymbol scopedSymbol -> relationNames.add(scopedSymbol.relation());
                case SelectSymbol selectSymbol -> selectSymbol.relation().accept(RELATION_TABLE_IDENT_EXTRACTOR, relationNames);
                case Reference ref -> relationNames.add(ref.ident().tableIdent());
                default -> {
                }
            }
            return false;
        });
        return relationNames;
    }

    private static class TableIdentRelationVisitor extends AnalyzedRelationVisitor<Collection<RelationName>, Void> {

        @Override
        protected Void visitAnalyzedRelation(AnalyzedRelation relation, Collection<RelationName> context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "AnalyzedRelation '%s' not supported", relation.getClass()));
        }

        @Override
        public Void visitTableRelation(TableRelation tableRelation, Collection<RelationName> context) {
            context.add(tableRelation.tableInfo().ident());
            return null;
        }

        @Override
        public Void visitDocTableRelation(DocTableRelation relation, Collection<RelationName> context) {
            context.add(relation.tableInfo().ident());
            return null;
        }

        @Override
        public Void visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, Collection<RelationName> context) {
            return null;
        }

        @Override
        public Void visitQueriedSelectRelation(QueriedSelectRelation relation, Collection<RelationName> context) {
            for (AnalyzedRelation analyzedRelation : relation.from()) {
                analyzedRelation.accept(this, context);
            }
            return null;
        }
    }
}

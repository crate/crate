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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.FetchReference;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.MatchPredicate;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class TableIdentsExtractor {

    private static final TableIdentRelationVisitor RELATION_TABLE_IDENT_EXTRACTOR = new TableIdentRelationVisitor();
    private static final TableIdentSymbolVisitor SYMBOL_TABLE_IDENT_EXTRACTOR = new TableIdentSymbolVisitor();

    /**
     * Extracts all table idents from all given symbols if possible (some symbols don't provide any table ident info)
     */
    public static Collection<RelationName> extract(Iterable<? extends Symbol> symbols) {
        Collection<RelationName> relationNames = new HashSet<>();
        for (Symbol symbol : symbols) {
            Collection<RelationName> relationNameList = symbol.accept(SYMBOL_TABLE_IDENT_EXTRACTOR, null);
            if (relationNameList != null) {
                relationNames.addAll(relationNameList);
            }
        }
        return relationNames;
    }

    /**
     * Extracts all table idents from the given symbol if possible (some symbols don't provide any table ident info)
     */
    public static Iterable<RelationName> extract(Symbol symbol) {
        return symbol.accept(SYMBOL_TABLE_IDENT_EXTRACTOR, null);
    }

    private static class TableIdentSymbolVisitor extends SymbolVisitor<Void, Collection<RelationName>> {

        @Override
        protected Collection<RelationName> visitSymbol(Symbol symbol, Void context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "Symbol '%s' not supported", Symbol.class.getName()));
        }

        @Override
        public Collection<RelationName> visitAggregation(Aggregation symbol, Void context) {
            return extract(symbol.inputs());
        }

        @Override
        public Collection<RelationName> visitReference(Reference symbol, Void context) {
            return Collections.singletonList(symbol.ident().tableIdent());
        }

        @Override
        public Collection<RelationName> visitDynamicReference(DynamicReference symbol, Void context) {
            return visitReference(symbol, context);
        }

        @Override
        public Collection<RelationName> visitFunction(Function symbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<RelationName> visitLiteral(Literal symbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<RelationName> visitInputColumn(InputColumn inputColumn, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<RelationName> visitField(ScopedSymbol field, Void context) {
            return List.of(field.relation());
        }

        @Override
        public Collection<RelationName> visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
            Set<RelationName> relationNames = new HashSet<>();
            for (Map.Entry<Symbol, Symbol> entry : matchPredicate.identBoostMap().entrySet()) {
                relationNames.addAll(entry.getKey().accept(this, context));
                relationNames.addAll(entry.getValue().accept(this, context));
            }
            relationNames.addAll(matchPredicate.queryTerm().accept(this, context));
            relationNames.addAll(matchPredicate.options().accept(this, context));
            return relationNames;
        }

        @Override
        public Collection<RelationName> visitFetchReference(FetchReference fetchReference, Void context) {
            return ((Symbol) fetchReference.ref()).accept(this, context);
        }

        @Override
        public Collection<RelationName> visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<RelationName> visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            return selectSymbol.relation().accept(RELATION_TABLE_IDENT_EXTRACTOR, context);
        }
    }

    private static class TableIdentRelationVisitor extends AnalyzedRelationVisitor<Void, Collection<RelationName>> {

        @Override
        protected Collection<RelationName> visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "AnalyzedRelation '%s' not supported", relation.getClass()));
        }

        @Override
        public Collection<RelationName> visitTableRelation(TableRelation tableRelation, Void context) {
            return Collections.singletonList(tableRelation.tableInfo().ident());
        }

        @Override
        public Collection<RelationName> visitDocTableRelation(DocTableRelation relation, Void context) {
            return Collections.singletonList(relation.tableInfo().ident());
        }

        @Override
        public Collection<RelationName> visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, Void context) {
            return List.of();
        }

        @Override
        public Collection<RelationName> visitQueriedSelectRelation(QueriedSelectRelation relation, Void context) {
            ArrayList<RelationName> names = new ArrayList<>();
            for (AnalyzedRelation analyzedRelation : relation.from()) {
                names.addAll(analyzedRelation.accept(this, context));
            }
            return names;
        }
    }
}

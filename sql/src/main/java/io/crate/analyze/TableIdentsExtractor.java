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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.TableFunctionRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.MatchPredicate;
import io.crate.analyze.symbol.ParameterSymbol;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.analyze.symbol.Value;
import io.crate.metadata.Reference;
import io.crate.metadata.TableIdent;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TableIdentsExtractor {

    private static final TableIdentRelationVisitor RELATION_VISITOR = new TableIdentRelationVisitor();
    private static final TableIdentSymbolVisitor SYMBOL_VISITOR = new TableIdentSymbolVisitor();

    /**
     * Extracts all table idents from all given symbols if possible (some symbols don't provide any table ident info)
     */
    public static Collection<TableIdent> extract(Iterable<? extends Symbol> symbols) {
        Collection<TableIdent> tableIdents = null;
        for (Symbol symbol : symbols) {
            Collection<TableIdent> tableIdentList = SYMBOL_VISITOR.process(symbol, null);
            //noinspection PointlessBooleanExpression
            if (tableIdentList != null && tableIdentList.isEmpty() == false) {
                if (tableIdents == null) {
                    tableIdents = new HashSet<>(tableIdentList.size());
                }
                tableIdents.addAll(tableIdentList);
            }
        }
        if (tableIdents == null) {
            return Collections.emptyList();
        }
        return tableIdents;
    }

    /**
     * Extracts all table idents from the given symbol if possible (some symbols don't provide any table ident info)
     */
    public static Iterable<TableIdent> extract(Symbol symbol) {
        return SYMBOL_VISITOR.process(symbol, null);
    }

    private static class TableIdentSymbolVisitor extends SymbolVisitor<Void, Collection<TableIdent>> {

        @Override
        protected Collection<TableIdent> visitSymbol(Symbol symbol, Void context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "Symbol '%s' not supported", Symbol.class.getName()));
        }

        @Override
        public Collection<TableIdent> visitAggregation(Aggregation symbol, Void context) {
            return extract(symbol.inputs());
        }

        @Override
        public Collection<TableIdent> visitReference(Reference symbol, Void context) {
            return Collections.singletonList(symbol.ident().tableIdent());
        }

        @Override
        public Collection<TableIdent> visitDynamicReference(DynamicReference symbol, Void context) {
            return visitReference(symbol, context);
        }

        @Override
        public Collection<TableIdent> visitFunction(Function symbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<TableIdent> visitLiteral(Literal symbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<TableIdent> visitInputColumn(InputColumn inputColumn, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<TableIdent> visitValue(Value symbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<TableIdent> visitField(Field field, Void context) {
            return RELATION_VISITOR.process(field.relation(), context);
        }

        @Override
        public Collection<TableIdent> visitMatchPredicate(MatchPredicate matchPredicate, Void context) {
            Set<TableIdent> tableIdents = new HashSet<>();
            for (Map.Entry<Field, Symbol> entry : matchPredicate.identBoostMap().entrySet()) {
                tableIdents.addAll(process(entry.getKey(), context));
                tableIdents.addAll(process(entry.getValue(), context));
            }
            tableIdents.addAll(process(matchPredicate.queryTerm(), context));
            tableIdents.addAll(process(matchPredicate.options(), context));
            return tableIdents;
        }

        @Override
        public Collection<TableIdent> visitFetchReference(FetchReference fetchReference, Void context) {
            return process(fetchReference.ref(), context);
        }

        @Override
        public Collection<TableIdent> visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
            return Collections.emptyList();
        }

        @Override
        public Collection<TableIdent> visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
            return RELATION_VISITOR.process(selectSymbol.relation(), context);
        }
    }

    private static class TableIdentRelationVisitor extends AnalyzedRelationVisitor<Void, Collection<TableIdent>> {

        @Override
        protected Collection<TableIdent> visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                "AnalyzedRelation '%s' not supported", relation.getClass()));
        }

        @Override
        public Collection<TableIdent> visitQueriedTable(QueriedTable table, Void context) {
            return Collections.singletonList(table.tableRelation.tableInfo().ident());
        }

        @Override
        public Collection<TableIdent> visitQueriedDocTable(QueriedDocTable table, Void context) {
            return process(table.tableRelation(), context);
        }

        @Override
        public Collection<TableIdent> visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, Void context) {
            Collection<TableIdent> tableIdents = new HashSet<>(multiSourceSelect.sources().size());
            for (AnalyzedRelation relation : multiSourceSelect.sources().values()) {
                tableIdents.addAll(process(relation, context));
            }
            return tableIdents;
        }

        @Override
        public Collection<TableIdent> visitTableRelation(TableRelation tableRelation, Void context) {
            return Collections.singletonList(tableRelation.tableInfo().ident());
        }

        @Override
        public Collection<TableIdent> visitDocTableRelation(DocTableRelation relation, Void context) {
            return Collections.singletonList(relation.tableInfo().ident());
        }

        @Override
        public Collection<TableIdent> visitTwoTableJoin(TwoTableJoin twoTableJoin, Void context) {
            Collection<TableIdent> tableIdents = new HashSet<>(2);
            tableIdents.addAll(process(twoTableJoin.left(), context));
            tableIdents.addAll(process(twoTableJoin.right(), context));
            return tableIdents;
        }

        @Override
        public Collection<TableIdent> visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, Void context) {
            return Collections.singletonList(tableFunctionRelation.tableInfo().ident());
        }

        @Override
        public Collection<TableIdent> visitQueriedSelectRelation(QueriedSelectRelation relation, Void context) {
            return process(relation.subRelation(), context);
        }
    }
}

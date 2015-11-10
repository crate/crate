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

package io.crate.testing;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Ordering;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;

import java.util.Collection;
import java.util.HashSet;

public class SQLPrinter {

    public static String print(Object o) {
        if (o instanceof QuerySpec) {
            return print((QuerySpec) o);
        } else if (o instanceof OrderBy) {
            return print((OrderBy) o);
        } else if (o instanceof Symbol) {
            return print((Symbol) o);
        } else if (o instanceof HashSet) {
            return print(Ordering.usingToString().sortedCopy((HashSet) o));
        } else if (o instanceof Collection) {
            return print((Collection<Symbol>) o);
        } else if (o == null) {
            return "null";
        }
        return o.toString();
    }

    public static String print(Collection<Symbol> symbols) {
        StringBuilder sb = new StringBuilder();
        SymbolPrinter visitor = new SymbolPrinter(sb);
        Context ctx = new Context();
        visitor.process(symbols, ctx);
        return sb.toString();
    }


    public static String print(Symbol symbol) {
        StringBuilder sb = new StringBuilder();
        SymbolPrinter visitor = new SymbolPrinter(sb);
        Context ctx = new Context();
        visitor.process(symbol, ctx);
        return sb.toString();
    }

    public static String print(OrderBy orderBy) {
        StringBuilder sb = new StringBuilder();
        SymbolPrinter visitor = new SymbolPrinter(sb);
        Context ctx = new Context();
        visitor.process(orderBy, ctx);
        return sb.toString();
    }

    public static String print(QuerySpec spec) {
        StringBuilder sb = new StringBuilder();
        SymbolPrinter visitor = new SymbolPrinter(sb);
        Context ctx = new Context();

        sb.append("SELECT ");
        visitor.process(spec.outputs(), ctx);

        if (spec.where().hasQuery()) {
            sb.append(" WHERE ");
            visitor.process(spec.where().query(), ctx);
        }
        if (spec.orderBy().isPresent()) {
            sb.append(" ORDER BY ");
            visitor.process(spec.orderBy().get(), ctx);
        }
        if (spec.limit().isPresent()) {
            sb.append(" LIMIT ");
            sb.append(spec.limit().get());
        }

        if (spec.offset() > 0) {
            sb.append(" OFFSET ");
            sb.append(spec.offset());
        }

        return visitor.sb.toString();
    }

    static class Context {

        BiMap<AnalyzedRelation, String> relNames = HashBiMap.create();
        Integer nextId = 0;


        String relIdent(AnalyzedRelation rel) {
            String name = relNames.get(rel);
            if (name != null) {
                return name;
            }
            String prefix = name = RelationNamer.INSTANCE.process(rel, null);
            Integer idx = 1;
            while (relNames.inverse().containsKey(name)) {
                name = prefix + (idx++).toString();
            }
            relNames.put(rel, name);
            return name;
        }

    }

    private static class RelationNamer extends AnalyzedRelationVisitor<Void, String> {

        static final RelationNamer INSTANCE = new RelationNamer();

        @Override
        protected String visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return relation.getClass().getCanonicalName();
        }

        @Override
        public String visitTableRelation(TableRelation tableRelation, Void context) {
            return tableRelation.tableInfo().ident().fqn();
        }

        @Override
        public String visitDocTableRelation(DocTableRelation relation, Void context) {
            return relation.tableInfo().ident().fqn();
        }

        @Override
        public String visitQueriedDocTable(QueriedDocTable table, Void context) {
            return table.tableRelation().tableInfo().ident().fqn();
        }

        @Override
        public String visitQueriedTable(QueriedTable table, Void context) {
            return table.tableRelation().tableInfo().ident().fqn();
        }
    }

    private static class SymbolPrinter extends SymbolVisitor<Context, Void> {

        private final StringBuilder sb;

        public SymbolPrinter(StringBuilder sb) {
            this.sb = sb;
        }

        public void process(Iterable<? extends Symbol> symbols, Context ctx) {
            boolean first = true;
            for (Symbol arg : symbols) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                process(arg, ctx);
            }
        }

        @Override
        public Void visitRelationColumn(RelationColumn relationColumn, Context context) {
            sb.append("RELCOL(");
            sb.append(relationColumn.relationName());
            sb.append(", ");
            sb.append(relationColumn.index());
            sb.append(")");
            return null;
        }

        @Override
        public Void visitInputColumn(InputColumn inputColumn, Context context) {
            sb.append("INPUT(");
            sb.append(inputColumn.index());
            sb.append(")");
            return null;
        }

        @Override
        public Void visitField(Field field, Context context) {
            sb.append(context.relIdent(field.relation()));
            sb.append('.');
            sb.append(field.path().outputName());
            return null;
        }

        @Override
        public Void visitFetchReference(FetchReference fetchReference, Context context) {
            sb.append("FETCH(");
            process(fetchReference.docId(), context);
            sb.append(", ");
            process(fetchReference.ref(), context);
            sb.append(")");
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, Context context) {

            if (symbol.info().ident().name().startsWith("op_")) {
                if (symbol.arguments().size() == 1) {
                    sb.append(symbol.info().ident().name().substring(3));
                    sb.append('(');
                    process(symbol.arguments().get(0), context);
                    sb.append(')');
                } else {
                    assert symbol.arguments().size() == 2;
                    process(symbol.arguments().get(0), context);
                    sb.append(" ");
                    sb.append(symbol.info().ident().name().substring(3));
                    sb.append(" ");
                    process(symbol.arguments().get(1), context);
                }
            } else {
                sb.append(symbol.info().ident().name());
                sb.append('(');
                process(symbol.arguments(), context);
                sb.append(')');
            }
            return null;
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            sb.append(SymbolFormatter.format(symbol));
            return null;
        }

        public void process(OrderBy orderBy, Context ctx) {
            int i = 0;
            for (Symbol symbol : orderBy.orderBySymbols()) {
                if (i > 0) {
                    sb.append(", ");
                }
                process(symbol, ctx);
                if (orderBy.reverseFlags()[i]) {
                    sb.append(" DESC");
                }
                Boolean nullsFirst = orderBy.nullsFirst()[i];
                if (nullsFirst != null) {
                    sb.append(" NULLS");
                    if (nullsFirst) {
                        sb.append(" FIRST");
                    } else {
                        sb.append(" LAST");
                    }
                }
                i++;
            }
        }
    }

}

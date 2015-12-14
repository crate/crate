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
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationPrinter;
import io.crate.analyze.symbol.*;
import io.crate.analyze.symbol.format.SymbolPrinter;

import java.util.Collection;
import java.util.HashSet;

public class SQLPrinter {

    private static final TestingSymbolPrinter TESTING_SYMBOL_PRINTER = new TestingSymbolPrinter();

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
        Context ctx = new Context();
        TESTING_SYMBOL_PRINTER.process(symbols, ctx);
        return ctx.printed();
    }


    public static String print(Symbol symbol) {
        Context ctx = new Context();
        TESTING_SYMBOL_PRINTER.process(symbol, ctx);
        return ctx.printed();
    }

    public static String print(OrderBy orderBy) {
        Context ctx = new Context();
        TESTING_SYMBOL_PRINTER.process(orderBy, ctx);
        return ctx.printed();
    }

    public static String print(QuerySpec spec) {
        StringBuilder sb = new StringBuilder();
        Context ctx = new Context(sb);

        sb.append("SELECT ");
        TESTING_SYMBOL_PRINTER.process(spec.outputs(), ctx);

        if (spec.where().hasQuery()) {
            sb.append(" WHERE ");
            TESTING_SYMBOL_PRINTER.process(spec.where().query(), ctx);
        }
        if (spec.orderBy().isPresent()) {
            sb.append(" ORDER BY ");
            TESTING_SYMBOL_PRINTER.process(spec.orderBy().get(), ctx);
        }
        if (spec.limit().isPresent()) {
            sb.append(" LIMIT ");
            sb.append(spec.limit().get());
        }

        if (spec.offset() > 0) {
            sb.append(" OFFSET ");
            sb.append(spec.offset());
        }

        return ctx.printed();
    }

    static class Context {

        private final StringBuilder sb;

        public Context() {
            this(new StringBuilder());
        }

        public Context(StringBuilder sb) {
            this.sb = sb;
        }

        public String printed() {
            return sb.toString();
        }

    }

    /**
     * produces same results as with {@link SymbolPrinter#printFullQualified(Symbol)} but is
     * able to format other symbols that {@link SymbolPrinter} is not able to.
     */
    private static class TestingSymbolPrinter extends SymbolVisitor<Context, Void> {

        public void process(Iterable<? extends Symbol> symbols, Context ctx) {
            boolean first = true;
            for (Symbol arg : symbols) {
                if (!first) {
                    ctx.sb.append(", ");
                }
                first = false;
                process(arg, ctx);
            }
        }

        @Override
        protected Void visitSymbol(Symbol symbol, Context context) {
            context.sb.append(SymbolPrinter.INSTANCE.printFullQualified(symbol));
            return null;
        }

        public void process(OrderBy orderBy, Context ctx) {
            int i = 0;
            for (Symbol symbol : orderBy.orderBySymbols()) {
                if (i > 0) {
                    ctx.sb.append(", ");
                }
                process(symbol, ctx);
                if (orderBy.reverseFlags()[i]) {
                    ctx.sb.append(" DESC");
                }
                Boolean nullsFirst = orderBy.nullsFirst()[i];
                if (nullsFirst != null) {
                    ctx.sb.append(" NULLS");
                    if (nullsFirst) {
                        ctx.sb.append(" FIRST");
                    } else {
                        ctx.sb.append(" LAST");
                    }
                }
                i++;
            }
        }
    }

}

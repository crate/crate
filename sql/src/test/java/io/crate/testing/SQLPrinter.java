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

import com.google.common.collect.Ordering;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.symbol.Symbol;
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
        StringBuilder sb = new StringBuilder();
        TESTING_SYMBOL_PRINTER.process(symbols, sb);
        return sb.toString();
    }


    public static String print(Symbol symbol) {
        StringBuilder sb = new StringBuilder();
        TESTING_SYMBOL_PRINTER.process(symbol, sb);
        return sb.toString();
    }

    public static String print(OrderBy orderBy) {
        StringBuilder sb = new StringBuilder();
        TESTING_SYMBOL_PRINTER.process(orderBy, sb);
        return sb.toString();
    }

    public static String print(QuerySpec spec) {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT ");
        TESTING_SYMBOL_PRINTER.process(spec.outputs(), sb);

        if (spec.where().hasQuery()) {
            sb.append(" WHERE ");
            TESTING_SYMBOL_PRINTER.process(spec.where().query(), sb);
        }
        if (spec.groupBy().isPresent()) {
            sb.append(" GROUP BY ");
            TESTING_SYMBOL_PRINTER.process(spec.groupBy().get(), sb);
        }
        if (spec.having().isPresent()) {
            sb.append(" HAVING ");
            TESTING_SYMBOL_PRINTER.process(spec.having().get().query(), sb);
        }
        if (spec.orderBy().isPresent()) {
            sb.append(" ORDER BY ");
            TESTING_SYMBOL_PRINTER.process(spec.orderBy().get(), sb);
        }
        if (spec.limit().isPresent()) {
            sb.append(" LIMIT ");
            sb.append(spec.limit().get());
        }
        if (spec.offset() > 0) {
            sb.append(" OFFSET ");
            sb.append(spec.offset());
        }

        return sb.toString();
    }

    /**
     * produces same results as with {@link SymbolPrinter#printFullQualified(Symbol)} but is
     * able to format other symbols that {@link SymbolPrinter} is not able to.
     */
    private static class TestingSymbolPrinter {

        public void process(Iterable<? extends Symbol> symbols, StringBuilder sb) {
            boolean first = true;
            for (Symbol arg : symbols) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                process(arg, sb);
            }
        }

        public void process(Symbol symbol, StringBuilder sb) {
            sb.append(SymbolPrinter.INSTANCE.printFullQualified(symbol));
        }

        public void process(OrderBy orderBy, StringBuilder sb) {
            int i = 0;
            for (Symbol symbol : orderBy.orderBySymbols()) {
                if (i > 0) {
                    sb.append(", ");
                }
                process(symbol, sb);
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

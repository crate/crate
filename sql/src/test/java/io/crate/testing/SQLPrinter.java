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
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;

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
        if (!spec.groupBy().isEmpty()) {
            sb.append(" GROUP BY ");
            TESTING_SYMBOL_PRINTER.process(spec.groupBy(), sb);
        }
        HavingClause having = spec.having();
        if (having != null) {
            sb.append(" HAVING ");
            TESTING_SYMBOL_PRINTER.process(having.query(), sb);
        }
        OrderBy orderBy = spec.orderBy();
        if (orderBy != null) {
            sb.append(" ORDER BY ");
            TESTING_SYMBOL_PRINTER.process(orderBy, sb);
        }
        Symbol limit = spec.limit();
        if (limit != null) {
            sb.append(" LIMIT ");
            sb.append(print(limit));
        }
        Symbol offset = spec.offset();
        if (offset != null) {
            sb.append(" OFFSET ");
            sb.append(print(offset));
        }
        return sb.toString();
    }

    /**
     * produces same results as with {@link SymbolPrinter#printQualified(Symbol)} but is
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
            sb.append(SymbolPrinter.INSTANCE.printQualified(symbol));
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

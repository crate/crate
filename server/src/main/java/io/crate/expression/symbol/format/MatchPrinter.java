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

package io.crate.expression.symbol.format;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Due to the match predicate not being an actual function, we have to manually print it here.
 */
public final class MatchPrinter {

    private MatchPrinter() {}

    public static void printMatchPredicate(Function matchPredicate, Style style, StringBuilder sb) {
        List<Symbol> arguments = matchPredicate.arguments();
        sb.append("MATCH((");
        printColumns(arguments.get(0), sb);
        sb.append("), ");
        // second argument (keyword)
        sb.append(arguments.get(1).toString(style));
        sb.append(") USING ");
        // third argument (method)
        // need to print as identifier without quotes
        sb.append((String) ((Literal<?>) arguments.get(2)).value());
        printProperties(arguments.get(3), sb);
    }

    @SuppressWarnings("unchecked")
    private static void printColumns(Symbol cols, StringBuilder sb) {
        // first argument (cols)
        var columnBootMap = (Map<String, Object>) ((Literal<?>) cols).value();
        Iterator<Map.Entry<String, Object>> entryIterator = columnBootMap.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, Object> entry = entryIterator.next();
            sb.append(entry.getKey());
            Object boost = entry.getValue();
            if (boost != null) {
                sb
                    .append(" ")
                    .append(boost);
            }
            if (entryIterator.hasNext()) {
                sb.append(", ");
            }
        }
    }

    private static void printProperties(Symbol propSymbol, StringBuilder sb) {
        // fourth argument (properties)
        var properties = (Map<?, ?>) ((Literal<?>) propSymbol).value();
        if (properties.isEmpty()) {
            return;
        }
        sb.append(" WITH (");
        var it = properties.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            sb.append(entry.getKey()).append(" = ");
            Object value = entry.getValue();
            if (value != null) {
                sb.append("'");
                sb.append(value);
                sb.append("'");
            }
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(")");
    }
}

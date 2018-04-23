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

package io.crate.expression.symbol.format;

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import org.apache.lucene.util.BytesRef;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Due to the match predicate not being an actual function, we have to manually print it here.
 */
class MatchPrinter {

    static void printMatchPredicate(Function matchPredicate,
                                    SymbolPrinterContext context,
                                    SymbolPrinter.SymbolPrintVisitor symbolPrintVisitor) {
        StringBuilder sb = context.builder;
        List<Symbol> arguments = matchPredicate.arguments();

        sb.append("MATCH((");
        printColumns(arguments.get(0), sb);
        sb.append("), ");
        printKeyWord(arguments.get(1), symbolPrintVisitor, context);
        sb.append(") USING ");
        printMethod(arguments.get(2), sb);
        printProperties(arguments.get(3), sb);
    }

    private static void printColumns(Symbol cols, StringBuilder sb) {
        // first argument (cols)
        Map columnBootMap = (Map) ((Literal) cols).value();
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

    private static void printKeyWord(Symbol keyword,
                                     SymbolPrinter.SymbolPrintVisitor symbolPrintVisitor,
                                     SymbolPrinterContext context) {
        // second argument (keyword)
        symbolPrintVisitor.process(keyword, context);
    }

    private static void printMethod(Symbol method, StringBuilder sb) {
        // third argument (method)
        // need to print as identifier without quotes
        sb.append(((BytesRef) ((Literal) method).value()).utf8ToString());
    }

    private static void printProperties(Symbol propSymbol, StringBuilder sb) {
        // fourth argument (properties)
        Map properties = (Map) ((Literal) propSymbol).value();
        if (properties.isEmpty()) {
            return;
        }
        sb.append(" WITH (");
        Iterator<Map.Entry> propertiesIterator = properties.entrySet().iterator();
        while (propertiesIterator.hasNext()) {
            Map.Entry entry = propertiesIterator.next();
            sb.append(entry.getKey()).append(" = ");
            Object value = entry.getValue();
            if (value != null) {
                sb.append("'");
                if (value instanceof BytesRef) {
                    sb.append(((BytesRef) value).utf8ToString());
                } else {
                    sb.append(value);
                }
                sb.append("'");
            }
            if (propertiesIterator.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(")");
    }

}

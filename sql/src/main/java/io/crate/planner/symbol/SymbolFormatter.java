/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.symbol;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.doc.DocSchemaInfo;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class SymbolFormatter extends SymbolVisitor<Void, String> {

    private static final SymbolFormatter INSTANCE = new SymbolFormatter();

    private static final Joiner argJoiner = Joiner.on(", ");

    private SymbolFormatter() {}

    public static String format(String messageTmpl, Symbol ... symbols) {
        Object[] formattedSymbols = new String[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            Symbol s = symbols[i];
            if (s == null) {
                formattedSymbols[i] = "null";
            } else {
                formattedSymbols[i] = s.accept(INSTANCE, null);
            }
        }
        return String.format(Locale.ENGLISH, messageTmpl, formattedSymbols);
    }

    private static String format(String tmpl, Object ... args) {
        return String.format(Locale.ENGLISH, tmpl, args);
    }

    public static String format(Symbol symbol) {
        return symbol.accept(INSTANCE, null);
    }

    @Override
    protected String visitSymbol(Symbol symbol, Void context) {
        return symbol.toString();
    }

    @Override
    public String visitAggregation(Aggregation symbol, Void context) {
        return format("%s(%s)",
                symbol.functionIdent().name(), argJoiner.join(symbol.functionIdent().argumentTypes()));
    }

    @Override
    public String visitFunction(Function symbol, Void context) {
        return format("%s(%s)",
                symbol.info().ident().name(), argJoiner.join(symbol.info().ident().argumentTypes()));
    }

    @Override
    public String visitReference(Reference symbol, Void context) {
        StringBuilder builder = new StringBuilder();
        String schema = symbol.info().ident().tableIdent().schema();
        if (schema != null && !schema.equals(DocSchemaInfo.NAME)) {
            builder.append(symbol.info().ident().tableIdent().schema()).append(".");
        }
        return builder.append(symbol.info().ident().tableIdent().name())
               .append(".")
               .append(symbol.info().ident().columnIdent().sqlFqn()).toString();
    }

    @Override
    public String visitDynamicReference(DynamicReference symbol, Void context) {
        return visitReference(symbol, context);
    }

    @Override
    public String visitField(Field field, Void context) {
        return process(field.target(), context);
    }

    @Override
    public String visitLiteral(Literal symbol, Void context) {
        return formatValue(symbol.value());
    }

    private String formatValue(Object value) {
        return formatValue(value, new StringBuilder()).toString();
    }

    @SuppressWarnings("unchecked")
    private StringBuilder formatValue(Object value, StringBuilder builder) {
        if (value == null) {
            builder.append("NULL");
        } else if (value instanceof Map) {
            formatMap((Map<String, Object>) value, builder);
        } else if (value instanceof List) {
            formatList((List<?>) value, builder);
        } else if (value.getClass().isArray()) {
            formatList(ImmutableList.copyOf((Object[]) value), builder);
        } else if (value instanceof CharSequence || value instanceof Character) {
            builder.append("'").append(value.toString()).append("'");
        } else if (value instanceof BytesRef) {
            builder.append("'").append(((BytesRef)value).utf8ToString()).append("'");
        } else {
            builder.append(value.toString());
        }
        return builder;
    }

    private void formatMap(Map<String, Object> map, StringBuilder builder) {
        builder.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : new TreeMap<>(map).entrySet()) {
            if (!first) {
                builder.append(", ");
            } else {
                first = false;
            }
            builder.append("'").append(entry.getKey()).append("': ");
            formatValue(entry.getValue(), builder);
        }
        builder.append("}");
    }

    private void formatList(List<?> value, StringBuilder builder) {
        builder.append('[');
        boolean first = true;
        for (Object elem : value) {
            if (!first) {
                builder.append(", ");
            } else {
                first = false;
            }
            formatValue(elem, builder);
        }
        builder.append(']');
    }
}

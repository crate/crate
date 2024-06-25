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

package io.crate.expression.symbol;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.common.collections.Lists;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.types.DataType;

public final class Symbols {

    private Symbols() {}

    public static List<DataType<?>> typeView(List<? extends Symbol> symbols) {
        return Lists.mapLazy(symbols, Symbol::valueType);
    }

    public static Streamer<?>[] streamerArray(Symbol[] symbols) {
        Streamer<?>[] streamers = new Streamer<?>[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            streamers[i] = symbols[i].valueType().streamer();
        }
        return streamers;
    }

    public static Streamer<?>[] streamerArray(Collection<? extends Symbol> symbols) {
        Streamer<?>[] streamers = new Streamer<?>[symbols.size()];
        Iterator<? extends Symbol> iter = symbols.iterator();
        for (int i = 0; i < symbols.size(); i++) {
            streamers[i] = iter.next().valueType().streamer();
        }
        return streamers;
    }

    @Nullable
    public static <V> V lookupValueByColumn(Map<? extends Symbol, V> valuesBySymbol, ColumnIdent column) {
        for (Map.Entry<? extends Symbol, V> entry : valuesBySymbol.entrySet()) {
            Symbol key = entry.getKey();
            if (key instanceof Reference ref && ref.column().equals(column)) {
                return entry.getValue();
            }
            if (key instanceof ScopedSymbol scopedSymbol && scopedSymbol.column().equals(column)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * returns true if any of the symbols contains the given column
     */
    public static boolean containsColumn(Iterable<? extends Symbol> symbols, ColumnIdent path) {
        for (Symbol symbol : symbols) {
            if (symbol.hasColumn(path)) {
                return true;
            }
        }
        return false;
    }

    public static void toStream(Collection<? extends Symbol> symbols, StreamOutput out) throws IOException {
        out.writeVInt(symbols.size());
        for (Symbol symbol : symbols) {
            Symbol.toStream(symbol, out);
        }
    }

    public static List<Symbol> fromStream(StreamInput in) throws IOException {
        return in.readList(Symbol::fromStream);
    }


    /**
     * format symbols in simple style and use the formatted symbols as {@link String#format(Locale, String, Object...)} arguments
     * for the given <code>messageTmpl</code>.
     */
    public static String format(String messageTmpl, Symbol... symbols) {
        Object[] formattedSymbols = new String[symbols.length];
        for (int i = 0; i < symbols.length; i++) {
            Symbol s = symbols[i];
            if (s == null) {
                formattedSymbols[i] = "NULL";
            } else {
                formattedSymbols[i] = s.toString(Style.UNQUALIFIED);
            }
        }
        return String.format(Locale.ENGLISH, messageTmpl, formattedSymbols);
    }
}

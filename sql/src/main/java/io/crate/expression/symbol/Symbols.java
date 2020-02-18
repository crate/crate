/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.Lists;
import io.crate.Streamer;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class Symbols {

    private static final HasColumnVisitor HAS_COLUMN_VISITOR = new HasColumnVisitor();

    public static final Predicate<Symbol> IS_COLUMN = s -> s instanceof Field || s instanceof Reference;
    public static final Predicate<Symbol> IS_GENERATED_COLUMN = input -> input instanceof GeneratedReference;

    public static List<DataType> typeView(List<? extends Symbol> symbols) {
        return Lists.transform(symbols, Symbol::valueType);
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
            if (key instanceof Reference && ((Reference) key).column().equals(column)) {
                return entry.getValue();
            }
            if (key instanceof Field && ((Field) key).path().equals(column)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * returns true if the symbol contains the given columnIdent.
     * If symbol is a Function the function tree will be traversed
     */
    public static boolean containsColumn(@Nullable Symbol symbol, ColumnIdent path) {
        if (symbol == null) {
            return false;
        }
        return symbol.accept(HAS_COLUMN_VISITOR, path);
    }

    /**
     * returns true if any of the symbols contains the given column
     */
    public static boolean containsColumn(Iterable<? extends Symbol> symbols, ColumnIdent path) {
        for (Symbol symbol : symbols) {
            if (containsColumn(symbol, path)) {
                return true;
            }
        }
        return false;
    }

    public static void toStream(Collection<? extends Symbol> symbols, StreamOutput out) throws IOException {
        out.writeVInt(symbols.size());
        for (Symbol symbol : symbols) {
            toStream(symbol, out);
        }
    }

    public static void nullableToStream(@Nullable Symbol symbol, StreamOutput out) throws IOException {
        if (symbol == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(symbol.symbolType().ordinal());
            symbol.writeTo(out);
        }
    }

    public static void toStream(Symbol symbol, StreamOutput out) throws IOException {
        out.writeVInt(symbol.symbolType().ordinal());
        symbol.writeTo(out);
    }

    public static List<Symbol> listFromStream(StreamInput in) throws IOException {
        return in.readList(Symbols::fromStream);
    }

    @Nullable
    public static Symbol nullableFromStream(StreamInput in) throws IOException {
        boolean valuePresent = in.readBoolean();
        if (!valuePresent) {
            return null;
        }
        return fromStream(in);
    }

    public static Symbol fromStream(StreamInput in) throws IOException {
        return SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }

    public static ColumnIdent pathFromSymbol(Symbol symbol) {
        if (symbol instanceof Field) {
            return ((Field) symbol).path();
        } else if (symbol instanceof Reference) {
            return ((Reference) symbol).column();
        }
        return new ColumnIdent(SymbolPrinter.INSTANCE.printUnqualified(symbol));
    }

    private static class HasColumnVisitor extends SymbolVisitor<ColumnIdent, Boolean> {

        @Override
        protected Boolean visitSymbol(Symbol symbol, ColumnIdent column) {
            return false;
        }

        @Override
        public Boolean visitFunction(Function symbol, ColumnIdent column) {
            for (Symbol arg : symbol.arguments()) {
                if (arg.accept(this, column)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitFetchReference(FetchReference fetchReference, ColumnIdent column) {
            if (((Symbol) fetchReference.fetchId()).accept(this, column)) {
                return true;
            }
            return ((Symbol) fetchReference.ref()).accept(this, column);
        }

        @Override
        public Boolean visitField(Field field, ColumnIdent column) {
            return field.path().equals(column) || field.path().sqlFqn().equals(column.sqlFqn());
        }

        @Override
        public Boolean visitReference(Reference symbol, ColumnIdent column) {
            return column.equals(symbol.column());
        }
    }
}

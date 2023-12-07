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
import java.util.function.Predicate;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FunctionType;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;

public class Symbols {

    private static final HasColumnVisitor HAS_COLUMN_VISITOR = new HasColumnVisitor();

    public static final Predicate<Symbol> IS_COLUMN = s -> s instanceof ScopedSymbol || s instanceof Reference;
    public static final Predicate<Symbol> IS_GENERATED_COLUMN = input -> input instanceof GeneratedReference;
    public static final Predicate<Symbol> IS_CORRELATED_SUBQUERY = Symbols::isCorrelatedSubQuery;

    public static boolean isCorrelatedSubQuery(Symbol symbol) {
        return symbol instanceof SelectSymbol selectSymbol && selectSymbol.isCorrelated();
    }

    public static boolean containsCorrelatedSubQuery(Symbol symbol) {
        return SymbolVisitors.any(IS_CORRELATED_SUBQUERY, symbol);
    }

    public static boolean isAggregate(Symbol s) {
        return s instanceof Function fn && fn.signature().getKind() == FunctionType.AGGREGATE;
    }

    public static boolean isTableFunction(Symbol s) {
        return s instanceof Function fn && fn.signature().getKind() == FunctionType.TABLE;
    }

    public static List<DataType<?>> typeView(List<? extends Symbol> symbols) {
        return Lists2.mapLazy(symbols, Symbol::valueType);
    }

    public static List<TypeSignature> typeSignatureView(List<? extends Symbol> symbols) {
        return Lists2.mapLazy(symbols, s -> s.valueType().getTypeSignature());
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
        if (out.getVersion().before(Version.V_4_2_0) && symbol instanceof AliasSymbol aliasSymbol) {
            toStream(aliasSymbol.symbol(), out);
        } else {
            int ordinal = symbol.symbolType().ordinal();
            out.writeVInt(ordinal);
            symbol.writeTo(out);
        }
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
        if (symbol instanceof AliasSymbol aliasSymbol) {
            return new ColumnIdent(aliasSymbol.alias());
        } else if (symbol instanceof ScopedSymbol scopedSymbol) {
            return scopedSymbol.column();
        } else if (symbol instanceof Reference ref) {
            return ref.column();
        }
        return new ColumnIdent(symbol.toString(Style.UNQUALIFIED));
    }

    public static boolean isDeterministic(Symbol symbol) {
        boolean[] isDeterministic = new boolean[] { true };
        symbol.accept(new DefaultTraversalSymbolVisitor<boolean[], Void>() {

            @Override
            public Void visitFunction(io.crate.expression.symbol.Function func,
                                      boolean[] isDeterministic) {
                if (!func.signature().isDeterministic()) {
                    isDeterministic[0] = false;
                    return null;
                }
                return super.visitFunction(func, isDeterministic);
            }
        }, isDeterministic);
        return isDeterministic[0];
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

    public static Symbol unwrapReferenceFromCast(Symbol symbol) {
        if (symbol instanceof Function fn && fn.isCast()) {
            return fn.arguments().get(0);
        }
        return symbol;
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
            return fetchReference.ref().accept(this, column);
        }

        @Override
        public Boolean visitField(ScopedSymbol field, ColumnIdent column) {
            return field.column().equals(column);
        }

        @Override
        public Boolean visitReference(Reference symbol, ColumnIdent column) {
            return column.equals(symbol.column());
        }
    }

    public static ColumnDefinition<Expression> toColumnDefinition(Symbol symbol) {
        return new ColumnDefinition<>(
            pathFromSymbol(symbol).sqlFqn(), // allow ObjectTypes to return col name in subscript notation
            symbol.valueType().toColumnType(
                symbol instanceof Reference reference ? reference.columnPolicy() : ColumnPolicy.DYNAMIC,
                null),
            List.of());
    }
}

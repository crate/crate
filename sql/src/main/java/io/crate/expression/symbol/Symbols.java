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
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class Symbols {

    private static final HasColumnVisitor HAS_COLUMN_VISITOR = new HasColumnVisitor();
    private static final AllLiteralsMatcher ALL_LITERALS_MATCHER = new AllLiteralsMatcher();
    private static final FunctionCopyVisitor<Void> DEEP_COPY_VISITOR = new FunctionCopyVisitor<Void>() {
    };

    public static final Predicate<Symbol> IS_COLUMN = s -> s instanceof Field || s instanceof Reference;
    public static final Predicate<Symbol> IS_GENERATED_COLUMN = input -> input instanceof GeneratedReference;
    public static final UnaryOperator<Symbol> DEEP_COPY = s -> DEEP_COPY_VISITOR.process(s, null);

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


    /**
     * returns true if the symbol contains the given columnIdent.
     * If symbol is a Function the function tree will be traversed
     */
    public static boolean containsColumn(@Nullable Symbol symbol, Path path) {
        if (symbol == null) {
            return false;
        }
        return HAS_COLUMN_VISITOR.process(symbol, path);
    }

    /**
     * returns true if any of the symbols contains the given column
     */
    public static boolean containsColumn(Iterable<? extends Symbol> symbols, Path path) {
        for (Symbol symbol : symbols) {
            if (containsColumn(symbol, path)) {
                return true;
            }
        }
        return false;
    }

    public static boolean allLiterals(Symbol symbol) {
        assert symbol != null : "symbol must not be null";
        return ALL_LITERALS_MATCHER.process(symbol, null);
    }

    public static void toStream(Collection<? extends Symbol> symbols, StreamOutput out) throws IOException {
        out.writeVInt(symbols.size());
        for (Symbol symbol : symbols) {
            toStream(symbol, out);
        }
    }

    public static void toStream(Symbol symbol, StreamOutput out) throws IOException {
        out.writeVInt(symbol.symbolType().ordinal());
        symbol.writeTo(out);
    }

    public static List<Symbol> listFromStream(StreamInput in) throws IOException {
        return in.readList(Symbols::fromStream);
    }

    public static Symbol fromStream(StreamInput in) throws IOException {
        return SymbolType.VALUES.get(in.readVInt()).newInstance(in);
    }

    public static Path pathFromSymbol(Symbol symbol) {
        if (symbol instanceof Field) {
            return ((Field) symbol).path();
        } else if (symbol instanceof Reference) {
            return ((Reference) symbol).column();
        }
        return new OutputName(SymbolPrinter.INSTANCE.printUnqualified(symbol));
    }

    private static class HasColumnVisitor extends SymbolVisitor<Path, Boolean> {

        @Override
        protected Boolean visitSymbol(Symbol symbol, Path column) {
            return false;
        }

        @Override
        public Boolean visitFunction(Function symbol, Path column) {
            for (Symbol arg : symbol.arguments()) {
                if (process(arg, column)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitFetchReference(FetchReference fetchReference, Path column) {
            if (process(fetchReference.fetchId(), column)) {
                return true;
            }
            return process(fetchReference.ref(), column);
        }

        @Override
        public Boolean visitField(Field field, Path column) {
            return field.path().equals(column) || field.path().outputName().equals(column.outputName());
        }

        @Override
        public Boolean visitReference(Reference symbol, Path column) {
            return column.equals(symbol.column());
        }
    }

    /**
     * Returns true if all symbols in an expression are literals and false otherwise.
     */
    private static class AllLiteralsMatcher extends SymbolVisitor<Void, Boolean> {

        @Override
        public Boolean visitFunction(Function function, Void context) {
            return function.arguments().stream().allMatch(arg -> process(arg, null));
        }

        @Override
        public Boolean visitLiteral(Literal symbol, Void context) {
            return true;
        }

        @Override
        public Boolean visitSymbol(Symbol symbol, Void context) {
            return false;
        }
    }

}

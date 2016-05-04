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

package io.crate.analyze.symbol;

import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class Symbol implements Streamable {

    public static boolean isLiteral(Symbol symbol, DataType expectedType) {
        return symbol.symbolType() == SymbolType.LITERAL
                && symbol.valueType().equals(expectedType);
    }

    public interface SymbolFactory<T extends Symbol> {
        T newInstance();
    }

    public abstract SymbolType symbolType();

    public abstract <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

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
        int numSymbols = in.readVInt();
        if (numSymbols == 0) {
            return Collections.emptyList();
        }
        List<Symbol> symbols = new ArrayList<>(numSymbols);
        for (int i = 0; i < numSymbols; i++) {
            symbols.add(Symbol.fromStream(in));
        }
        return symbols;
    }

    public static Symbol fromStream(StreamInput in) throws IOException {
        Symbol symbol = SymbolType.values()[in.readVInt()].newInstance();
        symbol.readFrom(in);

        return symbol;
    }

    public abstract DataType valueType();
}

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

import io.crate.expression.symbol.format.Style;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public final class AliasSymbol extends Symbol {

    private final String alias;
    private final Symbol symbol;

    public AliasSymbol(String alias, Symbol symbol) {
        this.alias = alias;
        this.symbol = symbol;
    }

    public AliasSymbol(StreamInput in) throws IOException {
        alias = in.readString();
        symbol = Symbols.fromStream(in);
    }

    public Symbol symbol() {
        return symbol;
    }

    public String alias() {
        return alias;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias);
        Symbols.toStream(symbol, out);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.ALIAS;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitAlias(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return symbol.valueType();
    }

    @Override
    public String toString(Style style) {
        return symbol.toString(style) + " AS " + alias;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AliasSymbol that = (AliasSymbol) o;

        if (!alias.equals(that.alias)) {
            return false;
        }
        return symbol.equals(that.symbol);
    }

    @Override
    public int hashCode() {
        int result = alias.hashCode();
        result = 31 * result + symbol.hashCode();
        return result;
    }
}

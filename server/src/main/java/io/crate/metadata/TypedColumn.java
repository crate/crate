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

package io.crate.metadata;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.Style;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * A column with a type.
 * This is a low footprint alternative to {@link Reference}
 **/
public record TypedColumn(ColumnIdent name, DataType<?> type) implements Symbol {

    public static TypedColumn fromStream(StreamInput in) throws IOException {
        return new TypedColumn(new ColumnIdent(in), DataTypes.fromStream(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        name.writeTo(out);
        DataTypes.toStream(type, out);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.COLUMN;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitTypedColumn(this, context);
    }

    @Override
    public DataType<?> valueType() {
        return type;
    }

    @Override
    public String toString(Style style) {
        return name.quotedOutputName();
    }

    @Override
    public String toString() {
        return name.quotedOutputName();
    }
}

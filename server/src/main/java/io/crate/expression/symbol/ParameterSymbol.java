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

import io.crate.data.Row;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.format.Style;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.UndefinedType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;

public class ParameterSymbol implements Symbol {

    private final int index;
    private final DataType<?> boundType;
    private final DataType<?> internalType;

    public ParameterSymbol(int index, DataType<?> type) {
        this(index, type, type);
    }

    private ParameterSymbol(int index, DataType<?> boundType, DataType<?> internalType) {
        this.index = index;
        this.boundType = boundType;
        this.internalType = internalType;
    }

    public ParameterSymbol(StreamInput in) throws IOException {
        index = in.readVInt();
        boundType = DataTypes.fromStream(in);
        internalType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(index);
        DataTypes.toStream(boundType, out);
        DataTypes.toStream(internalType, out);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.PARAMETER;
    }

    @Override
    public ParameterSymbol cast(DataType<?> targetType, CastMode... modes) {
        return new ParameterSymbol(index, boundType, targetType);
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitParameterSymbol(this, context);
    }

    /**
     * Returns the bound type of the parameter. The type might be bound
     * upfront by the client or become defined during the Analysis.
     * @return The effective bound {@link DataType}.
     */
    public DataType<?> getBoundType() {
        if (boundType.id() == UndefinedType.ID) {
            return internalType;
        }
        return boundType;
    }

    @Override
    public DataType<?> valueType() {
        return internalType;
    }

    @Override
    public String toString() {
        return "$" + (index + 1);
    }

    @Override
    public String toString(Style style) {
        return "$" + (index + 1);
    }

    public int index() {
        return index;
    }

    @Override
    public long ramBytesUsed() {
        return IntegerType.INTEGER_SIZE + internalType.ramBytesUsed();
    }

    public Object bind(Row params) throws IllegalArgumentException {
        try {
            return params.get(index);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "The query contains a parameter placeholder $%d, but there are only %d parameter values",
                (index() + 1),
                params.numColumns()
            ));
        }
    }
}

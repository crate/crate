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
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A symbol which represents a column of a result array
 */
public class InputColumn implements Symbol, Comparable<InputColumn> {

    private final DataType<?> dataType;
    private final int index;

    /**
     * Map each data type to an {@link InputColumn} with an index corresponding to the data type's position in the list.
     */
    public static List<Symbol> mapToInputColumns(List<DataType<?>> dataTypes) {
        List<Symbol> inputColumns = new ArrayList<>(dataTypes.size());
        for (int i = 0; i < dataTypes.size(); i++) {
            inputColumns.add(new InputColumn(i, dataTypes.get(i)));
        }
        return inputColumns;
    }

    /**
     * Map each symbol to an {@link InputColumn} with an index corresponding to the symbol's position in the collection.
     */
    public static List<Symbol> mapToInputColumns(Collection<? extends Symbol> symbols) {
        List<Symbol> inputColumns = new ArrayList<>(symbols.size());
        int idx = 0;
        for (Symbol symbol : symbols) {
            inputColumns.add(new InputColumn(idx, symbol.valueType()));
            idx++;
        }
        return inputColumns;
    }

    public InputColumn(int index, @Nullable DataType<?> dataType) {
        assert index >= 0 : "index must be >= 0";
        this.index = index;
        this.dataType = Objects.requireNonNullElse(dataType, DataTypes.UNDEFINED);
    }

    public InputColumn(StreamInput in) throws IOException {
        index = in.readVInt();
        dataType = DataTypes.fromStream(in);
    }

    public InputColumn(int index) {
        this(index, null);
    }

    public int index() {
        return index;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.INPUT_COLUMN;
    }

    @Override
    public DataType<?> valueType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "INPUT(" + index + ")";
    }

    @Override
    public String toString(Style style) {
        return "INPUT(" + index + ")";
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitInputColumn(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(index);
        DataTypes.toStream(dataType, out);
    }

    @Override
    public int compareTo(InputColumn o) {
        return Integer.compare(index, o.index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InputColumn that = (InputColumn) o;

        return index == that.index;
    }

    @Override
    public int hashCode() {
        return index;
    }

    @Override
    public long ramBytesUsed() {
        return IntegerType.INTEGER_SIZE + dataType.ramBytesUsed();
    }
}

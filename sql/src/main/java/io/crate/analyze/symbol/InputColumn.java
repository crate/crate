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

import com.google.common.base.MoreObjects;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A symbol which represents a column of a result array
 */
public class InputColumn extends Symbol implements Comparable<InputColumn> {

    private static final IndexShifter INDEX_SHIFTER = new IndexShifter();

    private DataType dataType;
    private int index;

    public static List<Symbol> numInputs(int size) {
        List<Symbol> inputColumns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            inputColumns.add(new InputColumn(i));
        }
        return inputColumns;
    }

    public static List<Symbol> fromSymbols(List<? extends Symbol> symbols) {
        List<Symbol> inputColumns = new ArrayList<>(symbols.size());
        for (int i = 0; i < symbols.size(); i++) {
            inputColumns.add(new InputColumn(i, symbols.get(i).valueType()));
        }
        return inputColumns;
    }

    /**
     * Shifts the index at all given input columns right by 1
     */
    public static void shiftRight(@Nullable Collection<Symbol> symbols) {
        if (symbols == null) {
            return;
        }
        IndexShifterContext context = new IndexShifterContext();
        for (Symbol symbol : symbols) {
            INDEX_SHIFTER.process(symbol, context);
        }
    }

    /**
     * generate an inputColumn which points to some symbol that is part of sourceList
     */
    public static InputColumn fromSymbol(Symbol symbol, List<? extends Symbol> sourceList) {
        return new InputColumn(sourceList.indexOf(symbol), symbol.valueType());
    }

    public InputColumn(int index, @Nullable DataType dataType) {
        assert index >= 0;
        this.index = index;
        this.dataType = MoreObjects.firstNonNull(dataType, DataTypes.UNDEFINED);
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
    public DataType valueType() {
        return dataType;
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
    public int compareTo(@Nonnull InputColumn o) {
        return Integer.compare(index, o.index);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("index", index)
                          .add("type", dataType)
                          .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InputColumn that = (InputColumn) o;

        if (index != that.index) return false;
        if (!dataType.equals(that.dataType)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return index;
    }

    public static List<Symbol> fromTypes(List<DataType> dataTypes) {
        List<Symbol> inputColumns = new ArrayList<>(dataTypes.size());
        for (int i = 0; i < dataTypes.size(); i++) {
            inputColumns.add(new InputColumn(i, dataTypes.get(i)));
        }
        return inputColumns;
    }

    private static class IndexShifterContext {
        private List<InputColumn> alreadyShifted = new ArrayList<>();
    }

    /**
     * Shift all (nested) InputColumn indices right by one.
     * It will recognize already shifted ones.
     */
    private static class IndexShifter extends SymbolVisitor<IndexShifterContext, Void> {

        @Override
        public Void visitInputColumn(InputColumn inputColumn, IndexShifterContext context) {
            if (!context.alreadyShifted.contains(inputColumn)) {
                inputColumn.index += 1;
                context.alreadyShifted.add(inputColumn);
            }
            return null;
        }

        @Override
        public Void visitFetchReference(FetchReference fetchReference, IndexShifterContext context) {
            process(fetchReference.docId(), context);
            return null;
        }

        @Override
        public Void visitFunction(Function function, IndexShifterContext context) {
            for (Symbol symbol : function.arguments()) {
                process(symbol, context);
            }

            return null;
        }
    }
}

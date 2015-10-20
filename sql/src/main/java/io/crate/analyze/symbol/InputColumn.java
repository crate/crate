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

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * A symbol which represents a column of a result array
 */
public class InputColumn extends Symbol implements Comparable<InputColumn> {

    public static final SymbolFactory<InputColumn> FACTORY = new SymbolFactory<InputColumn>() {
        @Override
        public InputColumn newInstance() {
            return new InputColumn();
        }
    };

    private DataType dataType;

    private int index;

    public InputColumn(int index, @Nullable DataType dataType) {
        assert index >= 0;
        this.index = index;
        this.dataType = MoreObjects.firstNonNull(dataType, DataTypes.UNDEFINED);
    }

    public InputColumn(int index) {
        this(index, null);
    }

    public InputColumn() {

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
    public void readFrom(StreamInput in) throws IOException {
        index = in.readVInt();
        dataType = DataTypes.fromStream(in);
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

        return true;
    }

    @Override
    public int hashCode() {
        return index;
    }

}

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

package io.crate.planner.symbol;

import com.google.common.base.Objects;
import io.crate.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class ArrayLiteral extends Literal<Object[], ArrayLiteral> {

    private DataType itemType;
    private DataType valueType;
    private Object[] values;

    public static final SymbolFactory<ArrayLiteral> FACTORY = new SymbolFactory<ArrayLiteral>() {
        @Override
        public ArrayLiteral newInstance() {
            return new ArrayLiteral();
        }
    };

    private ArrayLiteral() {}

    public ArrayLiteral(DataType itemType, Object[] values) {
        assert values != null;
        this.itemType = itemType;
        this.valueType = itemType.arrayType();
        this.values = values;
    }

    @Override
    public int compareTo(ArrayLiteral o) {
        if (o == null) {
            return 1;
        } else if (this == o) {
            return 0;
        }

        return Integer.compare(values.length, o.values.length);
    }

    @Override
    public Object[] value() {
        return values;
    }

    @Override
    public DataType valueType() {
        return valueType;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.ARRAY_LITERAL;
    }

    public DataType itemType() {
        return itemType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Literal convertTo(DataType type) {
        if (valueType() == type) {
            return this;
        } else if (type == DataType.NOT_SUPPORTED) {
            return Null.INSTANCE;
        }

        DataType targetItemType = DataType.REVERSE_ARRAY_TYPE_MAP.get(type);
        if (values.length > 0) {
            Object[] newValues = new Object[values.length];
            Literal literal = Literal.forType(itemType(), values[0]);

            if (literal.valueType() == DataType.STRING) {
                for (int i = 0; i < values.length; i++) {
                    newValues[i] = ((StringLiteral)literal).convertValueTo(targetItemType, (String)values[i]);
                }
            } else {
                for (int i = 0; i < values.length; i++) {
                    newValues[i] = literal.convertValueTo(targetItemType, values[i]);
                }
            }
            return new ArrayLiteral(targetItemType, newValues);
        } else {
            return new ArrayLiteral(targetItemType, new Object[0]);
        }
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitArrayLiteral(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        itemType = DataType.fromStream(in);
        valueType = itemType.arrayType();
        values = (Object[])valueType.streamer().readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataType.toStream(itemType, out);
        valueType.streamer().writeTo(out, values);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("itemType", itemType)
                .add("numValues", values.length)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ArrayLiteral that = (ArrayLiteral) o;

        if (itemType != that.itemType) return false;
        if (!Arrays.equals(values, that.values)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + itemType.hashCode();
        result = 31 * result + Arrays.hashCode(values);
        return result;
    }
}

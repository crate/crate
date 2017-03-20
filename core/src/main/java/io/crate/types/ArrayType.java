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

package io.crate.types;

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class ArrayType extends DataType implements CollectionType, Streamer<Object[]> {

    public static final int ID = 100;
    private DataType<?> innerType;

    public ArrayType() {
    }

    public ArrayType(DataType<?> innerType) {
        this.innerType = innerType;
    }

    @Override
    public DataType<?> innerType() {
        return this.innerType;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public String getName() {
        return innerType.getName() + "_array";
    }

    @Override
    public Streamer<?> streamer() {
        return this;
    }

    @Override
    public Object[] value(Object value) {
        if (value == null) {
            return null;
        }
        Object[] result;
        if (value instanceof Collection) {
            Collection values = (Collection) value;
            result = new Object[values.size()];
            int idx = 0;
            for (Object o : values) {
                result[idx] = innerType.value(o);
                idx++;
            }
        } else {
            Object[] inputArray = (Object[]) value;
            result = new Object[inputArray.length];
            for (int i = 0; i < inputArray.length; i++) {
                result[i] = innerType.value(inputArray[i]);
            }
        }
        return result;
    }

    @Override
    public boolean isConvertableTo(DataType other) {
        return other.id() == UndefinedType.ID ||
               ((other instanceof CollectionType)
                && this.innerType.isConvertableTo(((CollectionType) other).innerType()));
    }

    @Override
    public int compareValueTo(Object val1, Object val2) {
        if (val2 == null) {
            return 1;
        } else if (this == val1) {
            return 0;
        }

        int size1 = val1 instanceof List ? ((List) val1).size() : ((Object[]) val1).length;
        int size2 = val2 instanceof List ? ((List) val2).size() : ((Object[]) val2).length;
        return Integer.compare(size1, size2);
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof ArrayType)) return -1;
        return Integer.compare(innerType.id(), ((ArrayType) o).innerType().id());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        innerType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(innerType, out);
    }

    @Override
    public Object[] readValueFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        // size of 0 is treated as null value so real size must be decreased by 1
        if (size == 0) {
            return null;
        }
        size--;
        Object[] array = new Object[size];
        for (int i = 0; i < size; i++) {
            array[i] = innerType.streamer().readValueFrom(in);
        }
        return array;
    }

    @Override
    public void writeValueTo(StreamOutput out, Object values) throws IOException {
        Object[] array = (Object[]) values;
        // write null as size 0, so increase real size by 1
        if (array == null) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(array.length + 1);
        for (Object value : array) {
            innerType.streamer().writeValueTo(out, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArrayType)) return false;
        if (!super.equals(o)) return false;

        ArrayType setType = (ArrayType) o;

        if (!innerType.equals(setType.innerType)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + innerType.hashCode();
        return result;
    }

}

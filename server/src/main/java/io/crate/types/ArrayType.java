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
import io.crate.protocols.postgres.parser.PgArrayParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * A type which contains a collection of elements of another type.
 */
public class ArrayType<T> extends DataType<List<T>> {

    public static final String NAME = "array";
    public static final int ID = 100;

    private final DataType<T> innerType;
    private Streamer<List<T>> streamer;

    /**
     * Construct a new Collection type
     * @param innerType The type of the elements inside the collection
     */
    public ArrayType(DataType<T> innerType) {
        this.innerType = Objects.requireNonNull(innerType, "Inner type must not be null.");
    }

    public static DataType<?> makeArray(DataType<?> valueType, int numArrayDimensions) {
        DataType<?> arrayType = valueType;
        for (int i = 0; i < numArrayDimensions; i++) {
            arrayType = new ArrayType<>(arrayType);
        }
        return arrayType;
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature(NAME, List.of(innerType.getTypeSignature()));
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        return List.of(innerType);
    }

    /**
     * Defaults to the {@link ArrayStreamer} but subclasses may override this method.
     */
    @Override
    public Streamer<List<T>> streamer() {
        if (streamer == null) {
            streamer = new ArrayStreamer<>(innerType);
        }
        return streamer;
    }

    public ArrayType(StreamInput in) throws IOException {
        innerType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(innerType, out);
    }

    @Override
    public String getName() {
        return innerType.getName() + "_" + NAME;
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.ARRAY;
    }

    public final DataType<T> innerType() {
        return innerType;
    }

    @Override
    public List<T> implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return convert(value, innerType::implicitCast);
    }

    @Override
    public List<T> explicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return convert(value, innerType::explicitCast);
    }

    @Override
    public List<T> value(Object value) {
        return convert(value, innerType::value);
    }

    @Nullable
    private static <T> List<T> convert(@Nullable Object value, Function<Object, T> innerType) {
        if (value == null) {
            return null;
        }
        ArrayList<T> result;
        if (value instanceof Collection) {
            Collection values = (Collection) value;
            result = new ArrayList<>(values.size());
            for (Object o : values) {
                result.add(innerType.apply(o));
            }
        } else if (value instanceof String) {
            //noinspection unchecked
            return (List<T>) PgArrayParser.parse(
                ((String) value).getBytes(StandardCharsets.UTF_8),
                bytes -> innerType.apply(new String(bytes, StandardCharsets.UTF_8))
            );
        } else {
            Object[] values = (Object[]) value;
            result = new ArrayList<>(values.length);
            for (Object o : values) {
                result.add(innerType.apply(o));
            }
        }
        return result;
    }

    @Override
    public int compareTo(DataType<?> o) {
        if (!(o instanceof ArrayType)) return -1;
        return innerType.compareTo(((ArrayType<?>) o).innerType);
    }

    @Override
    public int compare(List<T> val1, List<T> val2) {
        if (val2 == null) {
            return 1;
        } else if (val1 == null) {
            return -1;
        }
        if (val1.size() > val2.size()) {
            return 1;
        } else if (val2.size() > val1.size()) {
            return -1;
        }
        for (int i = 0; i < val1.size(); i++) {
            int cmp = Comparator.nullsFirst(innerType).compare(val1.get(i), val2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public boolean isConvertableTo(DataType<?> other, boolean explicitCast) {
        return other.id() == UndefinedType.ID || other.id() == GeoPointType.ID ||
               ((other instanceof ArrayType)
                && this.innerType.isConvertableTo(((ArrayType<?>) other).innerType(), explicitCast));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArrayType)) return false;
        if (!super.equals(o)) return false;

        ArrayType arrayType = (ArrayType) o;
        return innerType.equals(arrayType.innerType);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + innerType.hashCode();
        return result;
    }

    public static DataType<?> unnest(DataType<?> dataType) {
        while (dataType instanceof ArrayType) {
            dataType = ((ArrayType<?>) dataType).innerType();
        }
        return dataType;
    }

    static class ArrayStreamer<T> implements Streamer<List<T>> {

        private final DataType<T> innerType;

        ArrayStreamer(DataType<T> innerType) {
            this.innerType = innerType;
        }

        @Override
        public List<T> readValueFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            // size of 0 is treated as null value so real size must be decreased by 1
            if (size == 0) {
                return null;
            }
            size--;
            ArrayList<T> values = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                values.add(innerType.streamer().readValueFrom(in));
            }
            return values;
        }

        @Override
        public void writeValueTo(StreamOutput out, List<T> values) throws IOException {
            // write null as size 0, so increase real size by 1
            if (values == null) {
                out.writeVInt(0);
                return;
            }
            out.writeVInt(values.size() + 1);
            for (T value : values) {
                innerType.streamer().writeValueTo(out, value);
            }
        }
    }
}

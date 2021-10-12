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

package io.crate.types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import io.crate.Streamer;
import io.crate.common.collections.Lists2;
import io.crate.protocols.postgres.parser.PgArrayParser;
import io.crate.protocols.postgres.parser.PgArrayParsingException;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

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

    @SuppressWarnings("unchecked")
    public ArrayType(StreamInput in) throws IOException {
        innerType = (DataType<T>) DataTypes.fromStream(in);
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
        return convert(value, innerType, innerType::implicitCast);
    }

    @Override
    public List<T> explicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return convert(value, innerType, innerType::explicitCast);
    }

    @Override
    public List<T> sanitizeValue(Object value) {
        return convert(value, innerType, innerType::sanitizeValue);
    }

    public List<String> fromAnyArray(Object[] values) throws IllegalArgumentException {
        if (values == null) {
            return null;
        } else {
            ArrayList<String> array = new ArrayList<>(values.length);
            for (var value : values) {
                array.add(anyValueToString(value));
            }
            return array;
        }
    }

    public List<String> fromAnyArray(List<?> values) throws IllegalArgumentException {
        if (values == null) {
            return null;
        } else {
            ArrayList<String> array = new ArrayList<>(values.size());
            for (var value : values) {
                array.add(anyValueToString(value));
            }
            return array;
        }
    }

    @SuppressWarnings("unchecked")
    private String anyValueToString(Object value) {
        if (value == null) {
            return null;
        }
        try {
            if (value instanceof Map) {
                //noinspection unchecked
                return
                    Strings.toString(
                        XContentFactory.jsonBuilder().map((Map<String, ?>) value));
            } else if (value instanceof Collection) {
                var array = XContentFactory.jsonBuilder().startArray();
                for (var element : (Collection<?>) value) {
                    array.value(element);
                }
                array.endArray();
                return Strings.toString(array);
            } else {
                return value.toString();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private static <T> List<T> convert(@Nullable Object value, DataType<T> innerType, Function<Object, T> convertInner) {
        if (value == null) {
            return null;
        }
        if (value instanceof Collection<?> values) {
            return Lists2.map(values, convertInner);
        } else if (value instanceof String string) {
            byte[] utf8Bytes = string.getBytes(StandardCharsets.UTF_8);
            try {
                return (List<T>) PgArrayParser.parse(
                    utf8Bytes,
                    bytes -> convertInner.apply(new String(bytes, StandardCharsets.UTF_8))
                );
            } catch (PgArrayParsingException e) {
                if (innerType instanceof JsonType) {
                    try {
                        return (List<T>) parseJsonList(utf8Bytes);
                    } catch (IOException ioEx) {
                        throw new UncheckedIOException(ioEx);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot parse `" + value + "` as array", e);
                }
            }
        } else {
            return convertObjectArray((Object[]) value, convertInner);
        }
    }

    private static List<String> parseJsonList(byte[] utf8Bytes) throws IOException {
        XContentParser parser = JsonXContent.JSON_XCONTENT.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            utf8Bytes
        );
        return Lists2.map(parser.list(), StringType.INSTANCE::explicitCast);
    }

    private static <T> ArrayList<T> convertObjectArray(Object[] values, Function<Object, T> convertInner) {
        ArrayList<T> result = new ArrayList<>(values.length);
        for (Object o : values) {
            result.add(convertInner.apply(o));
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ArrayType<?> arrayType = (ArrayType<?>) o;
        return Objects.equals(innerType, arrayType.innerType);
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

    @Override
    public ColumnType<Expression> toColumnType(ColumnPolicy columnPolicy,
                                               @Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        return new CollectionColumnType<>(innerType.toColumnType(columnPolicy, convertChildColumn));
    }

    @Override
    public StorageSupport storageSupport() {
        return innerType.storageSupport();
    }
}

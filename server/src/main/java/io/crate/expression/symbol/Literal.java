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

import io.crate.data.Input;
import io.crate.expression.symbol.format.Style;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.Period;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;


public class Literal<T> implements Symbol, Input<T>, Comparable<Literal<T>> {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Literal.class);

    private final T value;
    private final DataType<T> type;

    public static final Literal<Object> NULL = new Literal<>(DataTypes.UNDEFINED, null);
    public static final Literal<Boolean> BOOLEAN_TRUE = new Literal<>(DataTypes.BOOLEAN, true);
    public static final Literal<Boolean> BOOLEAN_FALSE = new Literal<>(DataTypes.BOOLEAN, false);
    public static final Literal<Map<String, Object>> EMPTY_OBJECT = Literal.of(Collections.emptyMap());

    public static Collection<Literal> explodeCollection(Literal collectionLiteral) {
        if (!DataTypes.isArray(collectionLiteral.valueType())) {
            throw new IllegalArgumentException("collectionLiteral must have have an array type");
        }
        Iterable values;
        int size;
        Object literalValue = collectionLiteral.value();
        if (literalValue instanceof Collection) {
            values = (Iterable) literalValue;
            size = ((Collection) literalValue).size();
        } else {
            values = Arrays.asList((Object[]) literalValue);
            size = ((Object[]) literalValue).length;
        }

        List<Literal> literals = new ArrayList<>(size);
        for (Object value : values) {
            literals.add(new Literal<>(
                ((ArrayType) collectionLiteral.valueType()).innerType(),
                value
            ));
        }
        return literals;
    }

    public Literal(StreamInput in) throws IOException {
        //noinspection unchecked
        type = (DataType<T>) DataTypes.fromStream(in);
        value = type.streamer().readValueFrom(in);
    }

    protected Literal(DataType<T> type, T value) {
        assert typeMatchesValue(type, value) :
            String.format(Locale.ENGLISH, "value %s is not of type %s", value, type.getName());
        this.type = type;
        this.value = value;
    }

    private static <T> boolean typeMatchesValue(DataType<T> type, T value) {
        if (value == null) {
            return true;
        }
        if (type.id() == ObjectType.ID) {
            //noinspection unchecked
            Map<String, Object> mapValue = (Map<String, Object>) value;
            ObjectType objectType = ((ObjectType) type);
            for (String key : mapValue.keySet()) {
                DataType<?> innerType = objectType.innerType(key);
                //noinspection unchecked
                if (typeMatchesValue((DataType<Object>) innerType, mapValue.get(key)) == false) {
                    return false;
                }
            }
            // lets do the expensive "deep" map value conversion only after everything else succeeded
            Map<String, Object> safeValue = objectType.sanitizeType(value);
            return safeValue.size() == mapValue.size();
        }

        return Objects.equals(type.sanitizeType(value), value);
    }

    @Override
    public int compareTo(Literal<T> o) {
        return type.compare(value, o.value);
    }

    @Override
    public T value() {
        return value;
    }

    @Override
    public DataType<T> valueType() {
        return type;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.LITERAL;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        LiteralValueFormatter.format(value, sb);
        return sb.toString();
    }

    @Override
    public String toString(Style style) {
        return toString();
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + type.ramBytesUsed() + type.valueBytes(value);
    }

    @Override
    public int hashCode() {
        if (value == null) {
            return 0;
        }
        if (value.getClass().isArray()) {
            return Arrays.deepHashCode(((Object[]) value));
        }
        return value.hashCode();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Literal<?> literal = (Literal<?>) obj;
        if (valueType().equals(literal.valueType())) {
            DataType type = valueType();
            return Comparator.nullsFirst(type).compare(value, literal.value) == 0;
        }
        return false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(type, out);
        type.streamer().writeValueTo(out, value);
    }

    public static Literal<Map<String, Object>> of(Map<String, Object> value) {
        return new Literal<>(DataTypes.UNTYPED_OBJECT, value);
    }

    public static <T> Literal<List<T>> of(List<T> value, DataType<List<T>> dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Long> of(Long value) {
        return new Literal<>(DataTypes.LONG, value);
    }

    public static Literal<?> ofUnchecked(DataType<?> type, Object value) {
        return new Literal(type, value);
    }

    public static <T> Literal<T> of(DataType<T> type, T value) {
        return new Literal<>(type, value);
    }

    public static Literal<Integer> of(Integer value) {
        return new Literal<>(DataTypes.INTEGER, value);
    }

    public static Literal<String> of(String value) {
        return new Literal<>(DataTypes.STRING, value);
    }

    public static Literal<Boolean> of(Boolean value) {
        if (value == null) {
            return new Literal<>(DataTypes.BOOLEAN, null);
        }
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Literal<Double> of(Double value) {
        return new Literal<>(DataTypes.DOUBLE, value);
    }

    public static Literal<Float> of(Float value) {
        return new Literal<>(DataTypes.FLOAT, value);
    }

    public static Literal<Period> newInterval(Period value) {
        return new Literal<>(DataTypes.INTERVAL, value);
    }
}

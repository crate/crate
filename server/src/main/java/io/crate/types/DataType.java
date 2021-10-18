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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.Streamer;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

public abstract class DataType<T> implements Comparable<DataType<?>>, Writeable, Comparator<T> {

    /**
     * Type precedence ids which help to decide when a type can be cast
     * into another type without losing information (upcasting).
     *
     * Lower ordinal => Lower precedence
     * Higher ordinal => Higher precedence
     *
     * Precedence list inspired by
     * https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-type-precedence-transact-sql
     *
     */
    public enum Precedence {
        NOT_SUPPORTED,
        UNDEFINED,
        LITERAL,
        STRING,
        BYTE,
        BOOLEAN,
        SHORT,
        TIMETZ,
        INTEGER,
        REGPROC,
        REGCLASS,
        INTERVAL,
        DATE,
        TIMESTAMP_WITH_TIME_ZONE,
        TIMESTAMP,
        LONG,
        IP,
        FLOAT,
        DOUBLE,
        NUMERIC,
        ARRAY,
        SET,
        TABLE,
        GEO_POINT,
        OBJECT,
        UNCHECKED_OBJECT,
        GEO_SHAPE,
        CUSTOM,
    }

    public abstract int id();

    /**
     * Returns the precedence of the type which determines whether the
     * type should be preferred (higher precedence) or converted (lower
     * precedence) during type conversions.
     */
    public abstract Precedence precedence();

    public abstract String getName();

    public abstract Streamer<T> streamer();

    /**
     * Converts the {@code value} argument to the the value of the current
     * data type. The conversion succeeds only if the {@code value} to the
     * desired data type, otherwise {@link ClassCastException} is thrown.
     * <p>
     * Should be used only in the cast functions, but there are exceptions.
     *
     * @param value The value to cast to the target {@link DataType}.
     * @return The value casted the target {@link DataType}.
     * @throws ClassCastException       if the conversion between data types is not supported.
     * @throws IllegalArgumentException if the conversion is supported but the converted value
     *                                  violates pre-conditions of the target type.
     */
    public T implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        throw new UnsupportedOperationException("The cast operation for type `" + getName() + "` is not supported.");
    }

    /**
     * Must be used only in the cast functions. The explicit cast
     * falls back to the implicit cast if it is not overwritten by
     * a data type subclass.
     *
     * @param value The value to cast to the target {@link DataType}.
     * @return The value casted the target {@link DataType}.
     */
    public T explicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return implicitCast(value);
    }

    /**
     * To prepare a value of the same {@link DataType<T>} for insertion.
     *
     * @param value The value of the {@link DataType<T>}.
     * @return The prepared for insertion value of the {@link DataType<T>}.
     */
    public T valueForInsert(Object value) {
        return (T) value;
    }

    /**
     * Fixes the {@link DataType} of the input {@code value} when its type is
     * slightly different the target {@link DataType}.
     * <p>
     * For example, to fix a type read from source  where `integer` might have
     * been stored as `bigint`. This is mostly in case for the reference
     * resolvers and column expression implementations.
     *
     * @param value The value to sanitize to the target {@link DataType}.
     * @return The value of {@link DataType}.
     * @see DataType#implicitCast(Object)
     */
    public abstract T sanitizeValue(Object value);

    public TypeSignature getTypeSignature() {
        return new TypeSignature(getName());
    }

    public List<DataType<?>> getTypeParameters() {
        return Collections.emptyList();
    }

    /**
     * Returns true if this DataType precedes the supplied DataType.
     * @param other The other type to compare against.
     * @return True if the current type precedes, false otherwise.
     */
    public boolean precedes(DataType<?> other) {
        return this.precedence().ordinal() > other.precedence().ordinal();
    }

    /**
     * check whether a value of this type is convertible to <code>other</code>
     *
     * @param other the DataType to check conversion to
     * @return true or false
     */
    public boolean isConvertableTo(DataType<?> other, boolean explicitCast) {
        if (this.equals(other)) {
            return true;
        }
        Set<Integer> possibleConversions = DataTypes.ALLOWED_CONVERSIONS.get(id());
        //noinspection SimplifiableIfStatement
        if (possibleConversions == null) {
            return false;
        }
        return possibleConversions.contains(other.id());
    }

    @Override
    public int hashCode() {
        return id();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataType)) return false;

        DataType<?> that = (DataType<?>) o;
        return (id() == that.id());
    }

    @Override
    public int compareTo(DataType<?> o) {
        return Integer.compare(id(), o.id());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String toString() {
        return getName();
    }


    /**
     * Returns {@link StorageSupport} if the type can be used in DDL statements and data can be persisted to disk. Absent if storage is unsupported.
     **/
    @Nullable
    public StorageSupport<? super T> storageSupport() {
        return null;
    }

    public ColumnType<Expression> toColumnType(ColumnPolicy columnPolicy,
                                               @Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        assert getTypeParameters().isEmpty()
            : "If the type parameters aren't empty, `" + getClass().getSimpleName() + "` must override `toColumnType`";
        return new ColumnType<>(getName());
    }
}

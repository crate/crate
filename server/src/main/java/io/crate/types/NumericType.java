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

import io.crate.Streamer;
import io.crate.common.annotations.VisibleForTesting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class NumericType extends DataType<BigDecimal> implements Streamer<BigDecimal> {

    public static final int ID = 22;
    public static final NumericType INSTANCE = new NumericType(null, null); // unscaled

    public static NumericType of(int precision) {
        return new NumericType(precision, 0);
    }

    public static NumericType of(int precision, int scale) {
        return new NumericType(precision, scale);
    }

    public static DataType<?> of(List<Integer> parameters) {
        if (parameters.isEmpty() || parameters.size() > 2) {
            throw new IllegalArgumentException(
                "The numeric type support one or two parameter arguments, received: " +
                parameters.size()
            );
        }
        if (parameters.size() == 1) {
            return of(parameters.get(0));
        } else {
            return of(parameters.get(0), parameters.get(1));
        }
    }

    @Nullable
    private final Integer scale;
    @Nullable
    private final Integer precision;

    private NumericType(@Nullable Integer precision, @Nullable Integer scale) {
        this.precision = precision;
        this.scale = scale;
    }

    public NumericType(StreamInput in) throws IOException {
        this.precision = in.readOptionalVInt();
        this.scale = in.readOptionalVInt();
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.NUMERIC;
    }

    @Override
    public String getName() {
        return "numeric";
    }

    @Override
    public Streamer<BigDecimal> streamer() {
        return this;
    }

    @Override
    public BigDecimal implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        }

        var mathContext = mathContextOrDefault();
        BigDecimal bd;
        if (value instanceof Long
            || value instanceof Byte
            || value instanceof Integer
            || value instanceof Short) {
            bd = new BigDecimal(
                BigInteger.valueOf(((Number) value).longValue()),
                mathContext
            );
        } else if (value instanceof String
                   || value instanceof Float
                   || value instanceof Double
                   || value instanceof BigDecimal) {
            bd = new BigDecimal(
                value.toString(),
                mathContext
            );
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
        if (scale != null) {
            bd = bd.setScale(scale, mathContext.getRoundingMode());
        }
        return bd;
    }

    @Override
    public BigDecimal sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else {
            return (BigDecimal) value;
        }
    }

    @Override
    public BigDecimal valueForInsert(Object value) {
        throw new UnsupportedOperationException(
            getName() + " type cannot be used in insert statements");
    }

    /**
     * Returns the size of {@link BigDecimal} in bytes
     */
    public static long size(@Nonnull BigDecimal value) {
        // BigInteger overhead 20 bytes
        // BigDecimal overhead 16 bytes
        // size of unscaled value
        return 36 + value.unscaledValue().bitLength() / 8 + 1;
    }

    public static long sizeDiff(@Nonnull BigDecimal first, @Nonnull BigDecimal second) {
        return NumericType.size(first) - NumericType.size(second);
    }

    @VisibleForTesting
    @Nullable
    Integer scale() {
        return scale;
    }

    @VisibleForTesting
    @Nullable
    Integer precision() {
        return precision;
    }

    private MathContext mathContextOrDefault() {
        if (precision == null) {
            return MathContext.UNLIMITED;
        } else {
            return new MathContext(precision);
        }
    }

    private boolean unscaled() {
        return precision == null;
    }

    @Override
    public TypeSignature getTypeSignature() {
        if (unscaled()) {
            return super.getTypeSignature();
        } else {
            ArrayList<TypeSignature> parameters = new ArrayList<>();
            parameters.add(TypeSignature.of(precision));
            if (scale != null) {
                parameters.add(TypeSignature.of(scale));
            }
            return new TypeSignature(getName(), parameters);
        }
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        if (unscaled()) {
            return List.of();
        } else {
            if (scale != null) {
                return List.of(DataTypes.INTEGER);
            } else {
                return List.of(DataTypes.INTEGER, DataTypes.INTEGER);
            }
        }
    }

    @Override
    public int compare(BigDecimal o1, BigDecimal o2) {
        return o1.compareTo(o2);
    }

    @Override
    public BigDecimal readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            byte[] bytes = in.readByteArray();
            return new BigDecimal(
                new BigInteger(bytes),
                scale == null ? 0 : scale,
                mathContextOrDefault()
            );
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, BigDecimal v) throws IOException {
        if (v != null) {
            out.writeBoolean(true);
            out.writeByteArray(v.unscaledValue().toByteArray());
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(precision);
        out.writeOptionalVInt(scale);
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
        NumericType that = (NumericType) o;
        return Objects.equals(scale, that.scale) &&
               Objects.equals(precision, that.precision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), scale, precision);
    }
}

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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

public class NumericType extends DataType<BigDecimal> implements Streamer<BigDecimal> {

    public static final int ID = 22;
    public static final String NAME = "numeric";
    public static final NumericType INSTANCE = new NumericType(null, null); // unscaled

    public static DataType<?> of(List<Integer> parameters) {
        if (parameters.isEmpty() || parameters.size() > 2) {
            throw new IllegalArgumentException(
                "The numeric type support one or two parameter arguments, received: " +
                parameters.size()
            );
        }
        if (parameters.size() == 1) {
            return new NumericType(parameters.get(0), 0);
        } else {
            return new NumericType(parameters.get(0), parameters.get(1));
        }
    }

    @Nullable
    private final Integer scale;
    @Nullable
    private final Integer precision;

    public NumericType(@Nullable Integer precision, @Nullable Integer scale) {
        if (scale != null) {
            if (precision == null) {
                throw new IllegalArgumentException("If scale is set for NUMERIC, precision must be set too");
            }
            if (scale >= precision) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Scale of numeric must be less than the precision. NUMERIC(%d, %d) is unsupported.",
                    precision,
                    scale
                ));
            }
            if (scale < 0) {
                throw new IllegalArgumentException("Scale of NUMERIC must not be negative");
            }
        }
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
        return NAME;
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

        var mathContext = mathContext();
        BigDecimal bd;
        if (value instanceof BigDecimal bigDecimal) {
            bd = bigDecimal.round(mathContext);
        } else if (value instanceof String || value instanceof Float || value instanceof Double) {
            bd = new BigDecimal(value.toString(), mathContext);
        } else if (value instanceof Number number) {
            bd = new BigDecimal(BigInteger.valueOf(number.longValue()), mathContext);
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
        if (scale == null) {
            return bd;
        }
        return bd.setScale(scale, mathContext.getRoundingMode());
    }

    @Override
    public BigDecimal sanitizeValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            MathContext mathContext = mathContext();
            BigDecimal bigDecimal = new BigDecimal(str, mathContext);
            return scale == null
                ? bigDecimal
                : bigDecimal.setScale(scale, mathContext.getRoundingMode());
        }
        // Can be long if the value comes from ScoreDoc/sort-field
        // See NumericStorage+LuceneSort+OrderByCollectorExpression
        if (value instanceof Long longValue) {
            BigInteger bigInt = BigInteger.valueOf(longValue);
            return new BigDecimal(bigInt, scale == null ? 0 : scale, mathContext());
        }
        return (BigDecimal) value;
    }

    @Override
    public BigDecimal valueForInsert(BigDecimal value) {
        return value;
    }

    @Override
    public ColumnType<Expression> toColumnType(ColumnPolicy columnPolicy,
                                               @Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        if (scale == null) {
            if (precision == null) {
                return new ColumnType<>(getName());
            }
            return new ColumnType<>(getName(), List.of(precision));
        }
        return new ColumnType<>(getName(), List.of(precision, scale));
    }


    /**
     * Returns the size of {@link BigDecimal} in bytes
     */
    public static long size(@NotNull BigDecimal value) {
        // BigInteger overhead 20 bytes
        // BigDecimal overhead 16 bytes
        // size of unscaled value
        return 36 + value.unscaledValue().bitLength() / 8L + 1;
    }

    public static long sizeDiff(@NotNull BigDecimal first, @NotNull BigDecimal second) {
        return NumericType.size(first) - NumericType.size(second);
    }

    @Override
    public Integer numericPrecision() {
        return precision;
    }

    @Nullable
    public Integer scale() {
        return scale;
    }

    public MathContext mathContext() {
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
                mathContext()
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

    @Override
    public long valueBytes(BigDecimal value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        return size(value);
    }

    @Override
    @Nullable
    public StorageSupport<? super BigDecimal> storageSupport() {
        return new NumericStorage(this);
    }

    @Override
    public void addMappingOptions(Map<String, Object> mapping) {
        if (precision == null || scale == null) {
            // Scale would be lost with the current encoding schemes used in NumericStorage
            // The error is raised here to trigger this early on CREATE TABLE instead of
            // INSERT INTO
            throw new UnsupportedOperationException(
                "NUMERIC storage is only supported if precision and scale are specified");
        }
        if (maxBytes() > PointValues.MAX_NUM_BYTES) {
            throw new UnsupportedOperationException(
                "Precision for NUMERIC(" + precision + ") is too large. Only up to 38 can be stored");
        }
        mapping.put("precision", precision);
        mapping.put("scale", scale);
    }

    @Override
    public String toString() {
        if (getTypeParameters().isEmpty()) {
            return super.toString();
        }
        if (scale == null) {
            return "numeric(" + precision + ")";
        }
        return "numeric(" + precision + "," + scale + ")";
    }

    public int maxBytes() {
        if (precision == null) {
            return Integer.MAX_VALUE;
        } else {
            return (new BigInteger("9".repeat(precision)).bitLength() / 8) + 1;
        }
    }

    public BigInteger minValue() {
        if (precision == null) {
            throw new UnsupportedOperationException("Can't get min value for numeric type without precision");
        }
        return maxValue().negate();
    }

    public BigInteger maxValue() {
        if (precision == null) {
            throw new UnsupportedOperationException("Can't get max value for numeric type without precision");
        }
        return new BigInteger("9".repeat(precision));
    }
}

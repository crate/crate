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
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.execution.dml.FloatIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class FloatType extends DataType<Float> implements Streamer<Float>, FixedWidthType {

    public static final FloatType INSTANCE = new FloatType();
    public static final int ID = 7;
    public static final int FLOAT_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Float.class);
    private static final StorageSupport<Float> STORAGE = new StorageSupport<>(
        true,
        true,
        new EqQuery<Float>() {

            @Override
            public Query termQuery(String field, Float value, boolean hasDocValues, boolean isIndexed) {
                if (isIndexed) {
                    return FloatPoint.newExactQuery(field, value);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowExactQuery(field, NumericUtils.floatToSortableInt(value));
                }
                return null;
            }

            @Override
            public Query rangeQuery(String field,
                                    Float lowerTerm,
                                    Float upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper,
                                    boolean hasDocValues,
                                    boolean isIndexed) {
                float lower;
                if (lowerTerm == null) {
                    lower = Float.NEGATIVE_INFINITY;
                } else {
                    lower = includeLower ? lowerTerm : FloatPoint.nextUp(lowerTerm);
                }

                float upper;
                if (upperTerm == null) {
                    upper = Float.POSITIVE_INFINITY;
                } else {
                    upper = includeUpper ? upperTerm : FloatPoint.nextDown(upperTerm);
                }
                if (isIndexed) {
                    return FloatPoint.newRangeQuery(field, lower, upper);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowRangeQuery(
                        field,
                        NumericUtils.floatToSortableInt(lower),
                        NumericUtils.floatToSortableInt(upper));
                }
                return null;
            }

            @Override
            public Query termsQuery(String field, List<Float> nonNullValues, boolean hasDocValues, boolean isIndexed) {
                if (isIndexed) {
                    return FloatPoint.newSetQuery(field, nonNullValues);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowSetQuery(field, nonNullValues.stream().mapToLong(NumericUtils::floatToSortableInt).toArray());
                }
                return null;
            }
        }
    ) {

        @Override
        public ValueIndexer<Float> valueIndexer(RelationName table,
                                                Reference ref,
                                                Function<String, FieldType> getFieldType,
                                                Function<ColumnIdent, Reference> getRef) {
            return new FloatIndexer(ref, getFieldType.apply(ref.storageIdent()));
        }
    };

    private static final BigInteger MAX = BigDecimal.valueOf(Float.MAX_VALUE).toBigInteger();
    private static final BigInteger MIN = BigDecimal.valueOf(-Float.MAX_VALUE).toBigInteger();

    private FloatType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.FLOAT;
    }

    @Override
    public String getName() {
        return "real";
    }

    @Override
    public Streamer<Float> streamer() {
        return this;
    }

    @Override
    public Float implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Float f) {
            return f;
        } else if (value instanceof String s) {
            return Float.parseFloat(s);
        } else if (value instanceof BigDecimal bigDecimalValue) {
            if (MAX.compareTo(bigDecimalValue.toBigInteger()) <= 0
                || MIN.compareTo(bigDecimalValue.toBigInteger()) >= 0) {
                throw new IllegalArgumentException("float value out of range: " + value);
            }
            return bigDecimalValue.floatValue();
        } else if (value instanceof Number number) {
            float val = number.floatValue();
            if (Float.isInfinite(val) && !Double.isInfinite(number.doubleValue())) {
                throw new IllegalArgumentException("float value out of range: " + value);
            }
            return val;
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Float sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Float f) {
            return f;
        } else {
            return ((Number) value).floatValue();
        }
    }

    @Override
    public int compare(Float val1, Float val2) {
        return Float.compare(val1, val2);
    }

    @Override
    public Float readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readFloat();
    }

    @Override
    public void writeValueTo(StreamOutput out, Float v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeFloat(v);
        }
    }

    @Override
    public int fixedSize() {
        return FLOAT_SIZE;
    }

    @Override
    public StorageSupport<Float> storageSupport() {
        return STORAGE;
    }

    @Override
    public long valueBytes(Float value) {
        return FLOAT_SIZE;
    }
}

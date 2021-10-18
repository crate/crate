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

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.math.BigDecimal;

public class FloatType extends DataType<Float> implements Streamer<Float>, FixedWidthType {

    public static final FloatType INSTANCE = new FloatType();
    public static final int ID = 7;
    private static final int FLOAT_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Float.class);
    private static final StorageSupport<Float> STORAGE = new StorageSupport<>(
        true,
        true,
        new EqQuery<Float>() {

            @Override
            public Query termQuery(String field, Float value) {
                return FloatPoint.newExactQuery(field, value);
            }

            @Override
            public Query rangeQuery(String field,
                                    Float lowerTerm,
                                    Float upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper) {
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

                Query indexQuery = FloatPoint.newRangeQuery(field, lower, upper);
                Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, NumericUtils.floatToSortableInt(lower), NumericUtils.floatToSortableInt(upper));
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
        }
    );

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
        } else if (value instanceof Float) {
            return (Float) value;
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        } else if (value instanceof BigDecimal) {
            var bigDecimalValue = (BigDecimal) value;

            var MAX = BigDecimal.valueOf(Float.MAX_VALUE).toBigInteger();
            var MIN = BigDecimal.valueOf(-Float.MAX_VALUE).toBigInteger();
            if (MAX.compareTo(bigDecimalValue.toBigInteger()) <= 0
                || MIN.compareTo(bigDecimalValue.toBigInteger()) >= 0) {
                throw new IllegalArgumentException("float value out of range: " + value);
            }
            return bigDecimalValue.floatValue();
        } else if (value instanceof Number) {
            Number number = (Number) value;
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
        } else if (value instanceof Float) {
            return (Float) value;
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
}

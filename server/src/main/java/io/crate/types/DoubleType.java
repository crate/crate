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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;

public class DoubleType extends DataType<Double> implements FixedWidthType, Streamer<Double> {

    public static final DoubleType INSTANCE = new DoubleType();
    public static final int ID = 6;
    private static final int DOUBLE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Double.class);
    private static final StorageSupport<Double> STORAGE = new StorageSupport<>(
        true,
        true,
        new EqQuery<Double>() {

            @Override
            public Query termQuery(String field, Double value) {
                return DoublePoint.newExactQuery(field, value);
            }

            @Override
            public Query rangeQuery(String field,
                                    Double lowerTerm,
                                    Double upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper) {
                double lower;
                if (lowerTerm == null) {
                    lower = Double.NEGATIVE_INFINITY;
                } else {
                    lower = includeLower ? lowerTerm : DoublePoint.nextUp(lowerTerm);
                }

                double upper;
                if (upperTerm == null) {
                    upper = Double.POSITIVE_INFINITY;
                } else {
                    upper = includeUpper ? upperTerm : DoublePoint.nextDown(upperTerm);
                }
                Query indexQuery = DoublePoint.newRangeQuery(field, lower, upper);
                Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(
                    field,
                    NumericUtils.doubleToSortableLong(lower),
                    NumericUtils.doubleToSortableLong(upper)
                );
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
        }
    );

    private DoubleType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.DOUBLE;
    }

    @Override
    public String getName() {
        return "double precision";
    }

    @Override
    public Streamer<Double> streamer() {
        return this;
    }

    @Override
    public Double implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof String) {
            return Double.valueOf((String) value);
        } else if (value instanceof BigDecimal) {
            var bigDecimalValue = (BigDecimal) value;

            var DOUBLE_MAX = BigDecimal.valueOf(Double.MAX_VALUE).toBigInteger();
            var DOUBLE_MIN = BigDecimal.valueOf(-Double.MAX_VALUE).toBigInteger();
            if (DOUBLE_MAX.compareTo(bigDecimalValue.toBigInteger()) <= 0
                || DOUBLE_MIN.compareTo(bigDecimalValue.toBigInteger()) >= 0) {
                throw new IllegalArgumentException(getName() + " value out of range: " + value);
            }
            return bigDecimalValue.doubleValue();
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Double sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            return (Double) value;
        } else {
            return ((Number) value).doubleValue();
        }
    }

    @Override
    public int compare(Double val1, Double val2) {
        return Double.compare(val1, val2);
    }

    @Override
    public Double readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readDouble();
    }

    @Override
    public void writeValueTo(StreamOutput out, Double v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeDouble(v);
        }
    }

    @Override
    public int fixedSize() {
        return DOUBLE_SIZE;
    }

    @Override
    public StorageSupport<Double> storageSupport() {
        return STORAGE;
    }
}

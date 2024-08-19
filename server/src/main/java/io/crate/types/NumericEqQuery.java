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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;

public class NumericEqQuery {

    static EqQuery<BigDecimal> of(NumericType type) {
        Integer precision = type.numericPrecision();
        if (precision == null) {
            return new Unoptimized();
        } else if (precision <= NumericStorage.COMPACT_PRECISION) {
            return new Compact();
        } else {
            return new Large(type);
        }
    }

    static class Compact implements EqQuery<BigDecimal> {

        @Override
        @Nullable
        public Query termQuery(String field, BigDecimal value, boolean hasDocValues, boolean isIndexed) {
            if (isIndexed) {
                long longValue = value.unscaledValue().longValueExact();
                return LongPoint.newExactQuery(field, longValue);
            }
            if (hasDocValues) {
                long longValue = value.unscaledValue().longValueExact();
                return SortedNumericDocValuesField.newSlowExactQuery(field, longValue);
            }
            return null;
        }

        @Override
        @Nullable
        public Query rangeQuery(String field,
                                BigDecimal lowerTerm,
                                BigDecimal upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            long lower = lowerTerm == null
                ? NumericStorage.COMPACT_MIN_VALUE
                : lowerTerm.unscaledValue().longValueExact() + (includeLower ? 0 : + 1);
            long upper = upperTerm == null
                ? NumericStorage.COMPACT_MAX_VALUE
                : upperTerm.unscaledValue().longValueExact() + (includeUpper ? 0 : - 1);
            return LongPoint.newRangeQuery(field, lower, upper);
        }

        @Override
        @Nullable
        public Query termsQuery(String field,
                                List<BigDecimal> nonNullValues,
                                boolean hasDocValues,
                                boolean isIndexed) {
            if (isIndexed) {
                return LongPoint.newSetQuery(
                    field,
                    Lists.mapLazy(nonNullValues, x -> x.unscaledValue().longValueExact()));
            }
            if (hasDocValues) {
                return SortedNumericDocValuesField.newSlowSetQuery(
                    field,
                    nonNullValues.stream().mapToLong(x -> x.unscaledValue().longValueExact()).toArray()
                );
            }
            return null;
        }
    }

    static class Large implements EqQuery<BigDecimal> {

        private final NumericType type;

        private Large(NumericType type) {
            this.type = type;
        }

        @Override
        @Nullable
        public Query termQuery(String field, BigDecimal value, boolean hasDocValues, boolean isIndexed) {
            return rangeQuery(field, value, value, true, true, hasDocValues, isIndexed);
        }

        @Override
        @Nullable
        public Query rangeQuery(String field,
                                BigDecimal lowerTerm,
                                BigDecimal upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            if (isIndexed) {
                int maxBytes = type.maxBytes();
                byte[] lower = new byte[maxBytes];
                byte[] upper = new byte[maxBytes];

                BigInteger lowerInt = lowerTerm == null
                    ? type.minValue()
                    : (lowerTerm.unscaledValue().add(includeLower ? BigInteger.ZERO : BigInteger.ONE));
                BigInteger upperInt = upperTerm == null
                    ? type.maxValue()
                    : (upperTerm.unscaledValue().subtract(includeUpper ? BigInteger.ZERO : BigInteger.ONE));
                NumericUtils.bigIntToSortableBytes(lowerInt, maxBytes, lower, 0);
                NumericUtils.bigIntToSortableBytes(upperInt, maxBytes, upper, 0);
                return new PointRangeQuery(field, lower, upper, 1) {

                    @Override
                    protected String toString(int dimension, byte[] value) {
                        return NumericUtils.sortableBytesToBigInt(value, 0, maxBytes).toString();
                    }
                };
            }
            return null;
        }

        @Override
        @Nullable
        public Query termsQuery(String field,
                                List<BigDecimal> nonNullValues,
                                boolean hasDocValues,
                                boolean isIndexed) {
            if (isIndexed) {
                int maxBytes = type.maxBytes();
                BytesRef encoded = new BytesRef(new byte[maxBytes]);
                return new PointInSetQuery(
                    field,
                    1,
                    maxBytes,
                    new PointInSetQuery.Stream() {

                        int idx = 0;

                        @Override
                        public BytesRef next() {
                            if (idx == nonNullValues.size()) {
                                return null;
                            }
                            BigDecimal bigDecimal = nonNullValues.get(idx);
                            idx++;
                            NumericUtils.bigIntToSortableBytes(bigDecimal.unscaledValue(), maxBytes, encoded.bytes, 0);
                            return encoded;
                        }
                    }) {

                        @Override
                        protected String toString(byte[] value) {
                            return NumericUtils.sortableBytesToBigInt(value, 0, maxBytes).toString();
                        }
                };
            }
            return null;
        }

    }

    static class Unoptimized implements EqQuery<BigDecimal> {

        @Override
        @Nullable
        public Query termQuery(String field, BigDecimal value, boolean hasDocValues, boolean isIndexed) {
            return null;
        }

        @Override
        @Nullable
        public Query rangeQuery(String field,
                                BigDecimal lowerTerm,
                                BigDecimal upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            return null;
        }

        @Override
        @Nullable
        public Query termsQuery(String field,
                                List<BigDecimal> nonNullValues,
                                boolean hasDocValues,
                                boolean isIndexed) {
            return null;
        }
    }
}

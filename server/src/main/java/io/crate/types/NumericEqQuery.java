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
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchNoDocsQuery;
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
            return new Compact(type);
        } else {
            return new Large(type);
        }
    }

    static class Compact implements EqQuery<BigDecimal> {

        private final NumericType type;

        private Compact(NumericType type) {
            this.type = type;
        }

        @Override
        @Nullable
        public Query termQuery(String field, BigDecimal value, boolean hasDocValues, boolean isIndexed) {
            BigDecimal scaleMatched = value.setScale(Objects.requireNonNull(type.scale()), RoundingMode.DOWN);
            if (scaleMatched.compareTo(value) != 0) {
                return new MatchNoDocsQuery("The given value holds extra non-zero scales");
            }
            if (isIndexed) {
                long longValue = scaleMatched.unscaledValue().longValueExact();
                return LongPoint.newExactQuery(field, longValue);
            }
            if (hasDocValues) {
                long longValue = scaleMatched.unscaledValue().longValueExact();
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
            final long lower;
            if (lowerTerm == null) {
                lower = NumericStorage.COMPACT_MIN_VALUE;
            } else {
                lowerTerm = lowerTerm.setScale(Objects.requireNonNull(type.scale()), includeLower ? RoundingMode.CEILING : RoundingMode.FLOOR);
                lower = lowerTerm.unscaledValue().longValueExact() + (includeLower ? 0 : + 1);
            }
            final long upper;
            if (upperTerm == null) {
                upper = NumericStorage.COMPACT_MAX_VALUE;
            } else {
                upperTerm = upperTerm.setScale(Objects.requireNonNull(type.scale()), includeUpper ? RoundingMode.FLOOR : RoundingMode.CEILING);
                upper = upperTerm.unscaledValue().longValueExact() + (includeUpper ? 0 : - 1);
            }
            return LongPoint.newRangeQuery(field, lower, upper);
        }

        @Override
        @Nullable
        public Query termsQuery(String field,
                                List<BigDecimal> nonNullValues,
                                boolean hasDocValues,
                                boolean isIndexed) {
            var scaleMatched = filterOutOfBoundsAndSetScale(nonNullValues, Objects.requireNonNull(type.scale()));
            if (scaleMatched.isEmpty()) {
                return new MatchNoDocsQuery("The given values are out of bounds");
            }
            if (isIndexed) {
                return LongPoint.newSetQuery(
                    field,
                    Lists.mapLazy(scaleMatched, x -> x.unscaledValue().longValueExact()));
            }
            if (hasDocValues) {
                return SortedNumericDocValuesField.newSlowSetQuery(
                    field,
                    scaleMatched.stream().mapToLong(x -> x.unscaledValue().longValueExact()).toArray()
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
            BigDecimal scaleMatched = value.setScale(Objects.requireNonNull(type.scale()), RoundingMode.DOWN);
            if (scaleMatched.compareTo(value) != 0) {
                return new MatchNoDocsQuery("The given value holds extra non-zero scales");
            }
            return rangeQuery(field, scaleMatched, scaleMatched, true, true, hasDocValues, isIndexed);
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

                final BigInteger lowerInt;
                if (lowerTerm == null) {
                    lowerInt = type.minValue();
                } else {
                    lowerTerm = lowerTerm.setScale(Objects.requireNonNull(type.scale()), includeLower ? RoundingMode.CEILING : RoundingMode.FLOOR);
                    lowerInt = lowerTerm.unscaledValue().add(includeLower ? BigInteger.ZERO : BigInteger.ONE);
                }
                final BigInteger upperInt;
                if (upperTerm == null) {
                    upperInt = type.maxValue();
                } else {
                    upperTerm = upperTerm.setScale(Objects.requireNonNull(type.scale()), includeUpper ? RoundingMode.FLOOR : RoundingMode.CEILING);
                    upperInt = (upperTerm.unscaledValue().subtract(includeUpper ? BigInteger.ZERO : BigInteger.ONE));
                }
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
            var scaleMatched = filterOutOfBoundsAndSetScale(nonNullValues, Objects.requireNonNull(type.scale()));
            if (scaleMatched.isEmpty()) {
                return new MatchNoDocsQuery("The given values are out of bounds");
            }
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
                            if (idx == scaleMatched.size()) {
                                return null;
                            }
                            BigDecimal bigDecimal = scaleMatched.get(idx);
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

    private static List<BigDecimal> filterOutOfBoundsAndSetScale(List<BigDecimal> values, int scale) {
        return values.stream().map(val -> {
            var scaledDown = val.setScale(scale, RoundingMode.DOWN);
            return scaledDown.compareTo(val) == 0 ? scaledDown : null;
        }).filter(Objects::nonNull).toList();
    }
}

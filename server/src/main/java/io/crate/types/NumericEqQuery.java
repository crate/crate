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
import java.util.List;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.Query;
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
            return new Unoptimized();
        }
    }

    static class Compact implements EqQuery<BigDecimal> {

        static long MIN_VALUE = -999999999999999999L;
        static long MAX_VALUE = 999999999999999999L;

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
                ? MIN_VALUE
                : lowerTerm.unscaledValue().longValueExact() + (includeLower ? 0 : + 1);
            long upper = upperTerm == null
                ? MAX_VALUE
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
                return LongPoint.newSetQuery(field, Lists.mapLazy(nonNullValues, x -> x.unscaledValue().longValueExact()));
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

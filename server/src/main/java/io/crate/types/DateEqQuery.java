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

import java.util.List;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.Query;

public class DateEqQuery implements EqQuery<Long> {

    @Override
    public Query termQuery(String field, Long value, boolean hasDocValues, boolean isIndexed) {
        int epochDay = DateType.toEpochDay(value);
        if (isIndexed) {
            return IntPoint.newExactQuery(field, epochDay);
        }
        if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowExactQuery(field, epochDay);
        }
        return null;
    }

    @Override
    public Query rangeQuery(String field,
                            Long lowerTerm,
                            Long upperTerm,
                            boolean includeLower,
                            boolean includeUpper,
                            boolean hasDocValues,
                            boolean isIndexed) {
        int lower = Integer.MIN_VALUE;
        if (lowerTerm != null) {
            int lowerEpochDay = DateType.toEpochDay(lowerTerm);
            lower = includeLower ? lowerEpochDay : lowerEpochDay + 1;
        }
        int upper = Integer.MAX_VALUE;
        if (upperTerm != null) {
            int upperEpochDay = DateType.toEpochDay(upperTerm);
            upper = includeUpper ? upperEpochDay : upperEpochDay - 1;
        }
        if (isIndexed) {
            return IntPoint.newRangeQuery(field, lower, upper);
        }
        if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowRangeQuery(field, lower, upper);
        }
        return null;
    }

    @Override
    public Query termsQuery(String field, List<Long> nonNullValues, boolean hasDocValues, boolean isIndexed) {
        if (isIndexed) {
            return IntPoint.newSetQuery(field, nonNullValues.stream().mapToInt(DateType::toEpochDay).toArray());
        }
        if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowSetQuery(
                field,
                nonNullValues.stream().mapToLong(DateType::toEpochDay).toArray()
            );
        }
        return null;
    }
}

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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

public class IntEqQuery implements EqQuery<Number> {

    @Override
    public Query termQuery(String field, Number value, boolean hasDocValues, boolean isIndexed) {
        if (isIndexed) {
            return IntPoint.newExactQuery(field, value.intValue());
        }
        if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowExactQuery(field, value.intValue());
        }
        return null;
    }

    @Override
    public Query rangeQuery(String field,
                            Number lowerTerm,
                            Number upperTerm,
                            boolean includeLower,
                            boolean includeUpper,
                            boolean hasDocValues,
                            boolean isIndexed) {
        int lower = Integer.MIN_VALUE;
        if (lowerTerm != null) {
            lower = includeLower ? lowerTerm.intValue() : lowerTerm.intValue() + 1;
        }
        int upper = Integer.MAX_VALUE;
        if (upperTerm != null) {
            upper = includeUpper ? upperTerm.intValue() : upperTerm.intValue() - 1;
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
    public Query termsQuery(String field, List<Number> nonNullValues, boolean hasDocValues, boolean isIndexed) {
        if (isIndexed) {
            return new BooleanQuery.Builder()
                .add(IntPoint.newSetQuery(field, nonNullValues.stream().mapToInt(Number::intValue).toArray()), BooleanClause.Occur.FILTER)
                .add(new IntEqQuery().termQuery("_array_size_" + field, nonNullValues.size(), hasDocValues, isIndexed), BooleanClause.Occur.FILTER)
                .build();
            //return IntPoint.newSetQuery(field, nonNullValues.stream().mapToInt(Number::intValue).toArray());
        }
        if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowSetQuery(field, nonNullValues.stream().mapToLong(Number::longValue).toArray());
        }
        return null;
    }
}

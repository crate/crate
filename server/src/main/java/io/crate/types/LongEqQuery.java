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

import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.Query;

import io.crate.metadata.IndexType;

public class LongEqQuery implements EqQuery<Long> {

    @Override
    public Query termQuery(String field, Long value, boolean hasDocValues, IndexType indexType) {
        boolean isIndexed = indexType != IndexType.NONE;
        if (hasDocValues && isIndexed) {
            return LongField.newExactQuery(field, value);
        } else if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowExactQuery(field, value);
        } else if (isIndexed) {
            return LongPoint.newExactQuery(field, value);
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
                            IndexType indexType) {
        long lower = lowerTerm == null
            ? Long.MIN_VALUE
            : (includeLower ? lowerTerm : lowerTerm + 1);
        long upper = upperTerm == null
            ? Long.MAX_VALUE
            : (includeUpper ? upperTerm : upperTerm - 1);
        boolean isIndexed = indexType != IndexType.NONE;
        if (hasDocValues && isIndexed) {
            return LongField.newRangeQuery(field, lower, upper);
        } else if (hasDocValues) {
            return SortedNumericDocValuesField.newSlowRangeQuery(field, lower, upper);
        } else if (isIndexed) {
            return LongPoint.newRangeQuery(field, lower, upper);
        }
        return null;
    }
}

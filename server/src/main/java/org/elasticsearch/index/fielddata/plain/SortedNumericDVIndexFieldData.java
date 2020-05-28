/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NullValueOrder;
import org.elasticsearch.search.MultiValueMode;

import io.crate.expression.reference.doc.lucene.NullSentinelValues;

/**
 * FieldData backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see DocValuesType#SORTED_NUMERIC
 */
public class SortedNumericDVIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData {
    private final NumericType numericType;

    public SortedNumericDVIndexFieldData(Index index, String fieldNames, NumericType numericType) {
        super(index, fieldNames);
        if (numericType == null) {
            throw new IllegalArgumentException("numericType must be non-null");
        }
        this.numericType = numericType;
    }

    @Override
    public SortField sortField(NullValueOrder nullValueOrder, MultiValueMode sortMode, boolean reverse) {
        final SortField sortField;
        final SortedNumericSelector.Type selectorType = sortMode == MultiValueMode.MAX ?
            SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;

        SortField.Type reducedType;
        switch (numericType) {
            case FLOAT:
                reducedType = SortField.Type.FLOAT;
                sortField = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse, selectorType);
                break;

            case DOUBLE:
                reducedType = SortField.Type.DOUBLE;
                sortField = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse, selectorType);
                break;

            default:
                assert !numericType.isFloatingPoint();
                reducedType = SortField.Type.LONG;
                sortField = new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse, selectorType);
                break;
        }
        sortField.setMissingValue(NullSentinelValues.nullSentinelForReducedType(
            reducedType,
            nullValueOrder,
            reverse
        ));
        return sortField;
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }
}

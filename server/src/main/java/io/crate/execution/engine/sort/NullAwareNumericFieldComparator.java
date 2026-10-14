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

package io.crate.execution.engine.sort;

import java.io.IOException;
import java.util.BitSet;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.NumericUtils;

import io.crate.exceptions.ArrayViaDocValuesUnsupportedException;

public class NullAwareNumericFieldComparator extends FieldComparator<Double>
        implements LeafFieldComparator {

    private final BitSet nullBitmap;
    private final long[] values;
    private final boolean nullsAtMin;
    private final SortField.Type sortFieldType;

    private long bottomValue;
    private boolean bottomIsNull;
    private long topValue;
    private boolean topIsNull;
    private long currentValue;
    private boolean currentIsNull;

    private SortedNumericDocValues docValues;
    private final String fieldName;

    NullAwareNumericFieldComparator(String fieldName, int numHits, boolean nullsAtMin, SortField.Type sortFieldType) {
        this.values = new long[numHits];
        this.nullBitmap = new BitSet(numHits);
        this.nullsAtMin = nullsAtMin;
        this.sortFieldType = sortFieldType;
        this.fieldName = fieldName;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        this.docValues = DocValues.getSortedNumeric(context.reader(), fieldName);
        return this;
    }

    private void readDoc(int doc) throws IOException {
        if (docValues.advanceExact(doc) == false) {
            currentValue = 0;
            currentIsNull = true;
            return;
        }
        if (docValues.docValueCount() > 1) {
            throw new ArrayViaDocValuesUnsupportedException(fieldName);
        }
        currentValue = docValues.nextValue();
        currentIsNull = false;
    }

    private int compare(long leftVal, boolean leftIsNull, long rightValue, boolean rightIsNull) {
        if (leftIsNull) {
            if (rightIsNull) {
                return 0;
            }
            return nullsAtMin ? -1 : 1;
        } else if (rightIsNull) {
            return nullsAtMin ? 1 : -1;
        }
        return Long.compare(leftVal, rightValue);
    }

    @Override
    public void setBottom(int slot) {
        bottomValue = values[slot];
        bottomIsNull = nullBitmap.get(slot);
    }

    @Override
    public void setTopValue(Double value) {
        if (value == null) {
            topValue = 0;
            topIsNull = true;
        } else {
            topValue = sortableOf(value);
            topIsNull = false;
        }
    }

    @Override
    public void setScorer(Scorable scorer) {
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        readDoc(doc);
        values[slot] = currentValue;
        nullBitmap.set(slot, currentIsNull);
    }

    @Override
    public Double value(int slot) {
        if (nullBitmap.get(slot)) {
            return null;
        }
        return decode(values[slot]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        readDoc(doc);
        return compare(bottomValue, bottomIsNull, currentValue, currentIsNull);
    }

    @Override
    public int compareTop(int doc) throws IOException {
        readDoc(doc);
        return compare(topValue, topIsNull, currentValue, currentIsNull);
    }

    @Override
    public int compare(int slot1, int slot2) {
        return compare(
                values[slot1], nullBitmap.get(slot1),
                values[slot2], nullBitmap.get(slot2));
    }

    private long sortableOf(double value) {
        if (sortFieldType.equals(SortField.Type.FLOAT)) {
            return NumericUtils.floatToSortableInt((float) value);
        }
        return NumericUtils.doubleToSortableLong(value);
    }

    private double decode(long sortable) {
        if (sortFieldType.equals(SortField.Type.FLOAT)) {
            return NumericUtils.sortableIntToFloat((int) sortable);
        }
        return NumericUtils.sortableLongToDouble(sortable);
    }

}

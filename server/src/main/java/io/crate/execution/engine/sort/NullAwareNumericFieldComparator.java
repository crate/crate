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

import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.NumericUtils;

public abstract class NullAwareNumericFieldComparator extends FieldComparator<NullAwareNumber>
        implements LeafFieldComparator {

    protected final BitSet nullBitmap;
    protected final long[] values;
    protected final boolean nullsAtMin;
    protected final SortField.Type sortFieldType;

    protected long bottomValue;
    protected boolean bottomIsNull;
    protected long topValue;
    protected boolean topIsNull;
    protected long currentValue;
    protected boolean currentIsNull;

    NullAwareNumericFieldComparator(int numHits, boolean nullsAtMin, SortField.Type sortFieldType) {
        this.values = new long[numHits];
        this.nullBitmap = new BitSet(numHits);
        this.nullsAtMin = nullsAtMin;
        this.sortFieldType = sortFieldType;
    }

    protected abstract void readDoc(int doc) throws IOException;

    protected int compare(long leftVal, boolean leftIsNull, long rightValue, boolean rightIsNull) {
        if (leftIsNull || rightIsNull) {
            if (leftIsNull && rightIsNull) {
                return 0;
            }
            if (leftIsNull) {
                return nullsAtMin ? -1 : 1;
            }
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
    public void setTopValue(NullAwareNumber value) {
        if (value == null || value.isNull()) {
            topValue = 0;
            topIsNull = true;
        } else {
            topValue = sortableOf(value.value());
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
    public NullAwareNumber value(int slot) {
        if (nullBitmap.get(slot)) {
            return NullAwareNumber.nullValue();
        }
        return NullAwareNumber.of(decode(values[slot]));
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

    protected long sortableOf(double value) {
        if (sortFieldType.equals(SortField.Type.FLOAT)) {
            return NumericUtils.floatToSortableInt((float) value);
        }
        return NumericUtils.doubleToSortableLong(value);
    }

    protected double decode(long sortable) {
        if (sortFieldType.equals(SortField.Type.FLOAT)) {
            return NumericUtils.sortableIntToFloat((int) sortable);
        }
        return NumericUtils.sortableLongToDouble(sortable);
    }

}

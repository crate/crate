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

import io.crate.data.Input;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

/**
 * Comparator for sorting on generic Inputs (Scalar Functions mostly)
 */
class InputFieldComparator extends FieldComparator<Object> implements LeafFieldComparator {

    private final Object[] values;
    private final Input<?> input;
    private final List<? extends LuceneCollectorExpression<?>> collectorExpressions;
    private final Comparator<Object> comparator;
    private final @Nullable Object missingValue;
    private Object bottom;
    private Object top;

    InputFieldComparator(int numHits,
                         List<? extends LuceneCollectorExpression<?>> collectorExpressions,
                         Input<?> input,
                         Comparator<Object> comparator,
                         @Nullable Object missingValue) {
        this.collectorExpressions = collectorExpressions;
        this.comparator = comparator;
        this.missingValue = missingValue;
        this.values = new Object[numHits];
        this.input = input;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        for (int i = 0; i < collectorExpressions.size(); i++) {
            collectorExpressions.get(i).setNextReader(new ReaderContext(context));
        }
        return this;
    }

    @Override
    public int compare(int slot1, int slot2) {
        return comparator.compare(values[slot1], values[slot2]);
    }

    @Override
    public void setBottom(int slot) {
        bottom = values[slot];
    }

    @Override
    public void setTopValue(Object value) {
        top = value;
    }

    @Override
    public int compareBottom(int doc) throws IOException {
        for (int i = 0; i < collectorExpressions.size(); i++) {
            collectorExpressions.get(i).setNextDocId(doc);
        }
        return comparator.compare(bottom, getFirstNonNullOrNull(input.value(), missingValue));
    }

    @Nullable
    private static Object getFirstNonNullOrNull(Object first, Object second) {
        if (first != null) {
            return first;
        } else {
            return second;
        }
    }

    @Override
    public int compareTop(int doc) throws IOException {
        for (int i = 0; i < collectorExpressions.size(); i++) {
            collectorExpressions.get(i).setNextDocId(doc);
        }
        return comparator.compare(top, getFirstNonNullOrNull(input.value(), missingValue));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        for (int i = 0; i < collectorExpressions.size(); i++) {
            collectorExpressions.get(i).setNextDocId(doc);
        }
        Object value = input.value();
        if (value == null) {
            values[slot] = missingValue;
        } else {
            values[slot] = value;
        }
    }

    @Override
    public void setScorer(Scorable scorer) {
    }

    @Override
    public Object value(int slot) {
        return values[slot];
    }
}

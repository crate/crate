/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.sort;

import com.google.common.base.MoreObjects;
import io.crate.data.Input;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.types.DataType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

/**
 * Comparator for sorting on generic Inputs (Scalar Functions mostly)
 */
class InputFieldComparator extends FieldComparator implements LeafFieldComparator {

    private final Object[] values;
    private final Input input;
    private final Iterable<? extends LuceneCollectorExpression<?>> collectorExpressions;
    private final Object missingValue;
    private final DataType valueType;
    private Object bottom;
    private Object top;

    InputFieldComparator(int numHits,
                         Iterable<? extends LuceneCollectorExpression<?>> collectorExpressions,
                         Input input,
                         DataType valueType,
                         Object missingValue) {
        this.collectorExpressions = collectorExpressions;
        this.missingValue = missingValue;
        this.valueType = valueType;
        this.values = new Object[numHits];
        this.input = input;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
        for (LuceneCollectorExpression collectorExpression : collectorExpressions) {
            collectorExpression.setNextReader(context);
        }
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(int slot1, int slot2) {
        return valueType.compareValueTo(values[slot1], values[slot2]);
    }

    @Override
    public void setBottom(int slot) {
        bottom = values[slot];
    }

    @Override
    public void setTopValue(Object value) {
        top = value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareBottom(int doc) throws IOException {
        for (LuceneCollectorExpression collectorExpression : collectorExpressions) {
            collectorExpression.setNextDocId(doc);
        }
        return valueType.compareValueTo(bottom, MoreObjects.firstNonNull(input.value(), missingValue));
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTop(int doc) throws IOException {
        for (LuceneCollectorExpression collectorExpression : collectorExpressions) {
            collectorExpression.setNextDocId(doc);
        }
        return valueType.compareValueTo(top, MoreObjects.firstNonNull(input.value(), missingValue));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
        for (LuceneCollectorExpression collectorExpression : collectorExpressions) {
            collectorExpression.setNextDocId(doc);
        }
        Object value = input.value();
        if (value == null) {
            values[slot] = missingValue;
        } else {
            values[slot] = value;
        }
    }

    @Override
    public void setScorer(Scorer scorer) {
    }

    @Override
    public Object value(int slot) {
        return values[slot];
    }
}

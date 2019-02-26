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
package io.crate.es.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import io.crate.es.common.Nullable;
import io.crate.es.index.fielddata.FieldData;
import io.crate.es.index.fielddata.IndexFieldData;
import io.crate.es.index.fielddata.IndexNumericFieldData;
import io.crate.es.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for long values.
 */
public class LongValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;

    public LongValuesComparatorSource(IndexNumericFieldData indexFieldData, @Nullable Object missingValue, MultiValueMode sortMode) {
        super(missingValue, sortMode);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final Long dMissingValue = (Long) missingObject(missingValue, reversed);
        // NOTE: it's important to pass null as a missing value in the constructor so that
        // the comparator doesn't check docsWithField since we replace missing values in select()
        return new FieldComparator.LongComparator(numHits, null, null) {
            @Override
            protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                final SortedNumericDocValues values = indexFieldData.load(context).getLongValues();
                return FieldData.replaceMissing(sortMode.select(values), dMissingValue);
            }

        };
    }
}

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

package org.elasticsearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.NullValueOrder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for string/binary values.
 */
public class BytesRefFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexFieldData<?> indexFieldData;

    public BytesRefFieldComparatorSource(IndexFieldData<?> indexFieldData, NullValueOrder nullValueOrder, MultiValueMode sortMode) {
        super(nullValueOrder, sortMode);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.STRING;
    }

    @Override
    public Object missingValue(boolean reversed) {
        if (nullValueOrder == NullValueOrder.LAST ^ reversed) {
            return SortField.STRING_LAST;
        } else {
            return SortField.STRING_FIRST;
        }
    }

    protected SortedBinaryDocValues getValues(LeafReaderContext context) throws IOException {
        return indexFieldData.load(context).getBytesValues();
    }

    protected void setScorer(Scorable scorer) {
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final boolean sortMissingLast = nullValueOrder == NullValueOrder.LAST ^ reversed;
        final BytesRef missingBytes = (BytesRef) missingObject(nullValueOrder, reversed);
        if (indexFieldData instanceof IndexOrdinalsFieldData) {
            return new FieldComparator.TermOrdValComparator(numHits, null, sortMissingLast) {

                @Override
                protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
                    final SortedSetDocValues values = ((IndexOrdinalsFieldData) indexFieldData).load(context).getOrdinalsValues();
                    return sortMode.select(values);
                }

                @Override
                public void setScorer(Scorable scorer) {
                    BytesRefFieldComparatorSource.this.setScorer(scorer);
                }

            };
        }

        return new FieldComparator.TermValComparator(numHits, null, sortMissingLast) {

            @Override
            protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field) throws IOException {
                final SortedBinaryDocValues values = getValues(context);
                return sortMode.select(values, missingBytes);
            }

            @Override
            public void setScorer(Scorable scorer) {
                BytesRefFieldComparatorSource.this.setScorer(scorer);
            }

        };
    }
}

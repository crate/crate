/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.action.sql.query;

import io.crate.executor.transport.task.elasticsearch.SortOrder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;

/**
 * ComparatorSource for fields that don't have a backing FieldMapper and FieldCache.
 * This always returns the appropriate <code>missingValue</code>.
 *
 * Only used on shards with no values for the compared field.
 */
class NullFieldComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final SortField.Type sortFieldType;
    private final Object missingValue;
    private final static LeafFieldComparator LEAF_FIELD_COMPARATOR = new LeafFieldComparator() {
        @Override
        public void setBottom(int slot) {
        }

        @Override
        public int compareBottom(int doc) throws IOException {
            return 0;
        }

        @Override
        public int compareTop(int doc) throws IOException {
            return 0;
        }

        @Override
        public void copy(int slot, int doc) throws IOException {
        }

        @Override
        public void setScorer(Scorer scorer) {
        }
    };

    NullFieldComparatorSource(SortField.Type sortFieldType, boolean reversed, Boolean nullsFirst) {
        this.sortFieldType = sortFieldType;
        missingValue = missingObject(SortOrder.missing(reversed, nullsFirst), reversed);
    }

    @Override
    public SortField.Type reducedType() {
        return sortFieldType;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, int sortPos, boolean reversed) throws IOException {
        return new FieldComparator<Object>() {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return LEAF_FIELD_COMPARATOR;
            }

            @Override
            public int compare(int slot1, int slot2) {
                return 0;
            }

            @Override
            public void setTopValue(Object value) {
            }

            @Override
            public Object value(int slot) {
                return missingValue;
            }
        };
    }
}

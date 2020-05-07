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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.SortField;
import org.elasticsearch.index.IndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

/**
 * Thread-safe utility class that allows to get per-segment values via the
 * {@link #load(LeafReaderContext)} method.
 */
public interface IndexFieldData<FD extends AtomicFieldData> extends IndexComponent {

    class CommonSettings {
        public static final String SETTING_MEMORY_STORAGE_HINT = "memory_storage_hint";

        public enum MemoryStorageFormat {
            ORDINALS, PACKED, PAGED;

            public static MemoryStorageFormat fromString(String string) {
                for (MemoryStorageFormat e : MemoryStorageFormat.values()) {
                    if (e.name().equalsIgnoreCase(string)) {
                        return e;
                    }
                }
                return null;
            }
        }
    }

    /**
     * The field name.
     */
    String getFieldName();

    /**
     * Loads the atomic field data for the reader, possibly cached.
     */
    FD load(LeafReaderContext context);

    /**
     * Loads directly the atomic field data for the reader, ignoring any caching involved.
     */
    FD loadDirect(LeafReaderContext context) throws Exception;

    /**
     * Returns the {@link SortField} to used for sorting.
     */
    SortField sortField(NullValueOrder nullValueOrder, MultiValueMode sortMode, boolean reverse);

    /**
     * Clears any resources associated with this field data.
     */
    void clear();

    // we need this extended source we we have custom comparators to reuse our field data
    // in this case, we need to reduce type that will be used when search results are reduced
    // on another node (we don't have the custom source them...)
    abstract class XFieldComparatorSource extends FieldComparatorSource {

        protected final MultiValueMode sortMode;
        protected final NullValueOrder nullValueOrder;

        public XFieldComparatorSource(NullValueOrder nullValueOrder, MultiValueMode sortMode) {
            this.sortMode = sortMode;
            this.nullValueOrder = nullValueOrder;
        }

        public MultiValueMode sortMode() {
            return this.sortMode;
        }

        /** Return the missing object value according to the reduced type of the comparator. */
        public final Object missingObject(NullValueOrder nullValueOrder, boolean reversed) {
            final boolean min = nullValueOrder == NullValueOrder.FIRST ^ reversed;
            switch (reducedType()) {
                case INT:
                    return min ? Integer.MIN_VALUE : Integer.MAX_VALUE;
                case LONG:
                    return min ? Long.MIN_VALUE : Long.MAX_VALUE;
                case FLOAT:
                    return min ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
                case DOUBLE:
                    return min ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
                case STRING:
                case STRING_VAL:
                    return null;
                default:
                    throw new UnsupportedOperationException("Unsupported reduced type: " + reducedType());
            }
        }

        public abstract SortField.Type reducedType();

        /**
         * Return a missing value that is understandable by {@link SortField#setMissingValue(Object)}.
         * Most implementations return null because they already replace the value at the fielddata level.
         * However this can't work in case of strings since there is no such thing as a string which
         * compares greater than any other string, so in that case we need to return
         * {@link SortField#STRING_FIRST} or {@link SortField#STRING_LAST} so that the coordinating node
         * knows how to deal with null values.
         */
        public Object missingValue(boolean reversed) {
            return null;
        }
    }

    interface Builder {

        IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                             CircuitBreakerService breakerService, MapperService mapperService);
    }

    interface Global<FD extends AtomicFieldData> extends IndexFieldData<FD> {

        IndexFieldData<FD> loadGlobal(DirectoryReader indexReader);

        IndexFieldData<FD> localGlobalDirect(DirectoryReader indexReader) throws Exception;

    }

}

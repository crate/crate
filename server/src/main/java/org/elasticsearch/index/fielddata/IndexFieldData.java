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

    interface Builder {

        IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                             CircuitBreakerService breakerService, MapperService mapperService);
    }

    interface Global<FD extends AtomicFieldData> extends IndexFieldData<FD> {

        IndexFieldData<FD> loadGlobal(DirectoryReader indexReader);

        IndexFieldData<FD> localGlobalDirect(DirectoryReader indexReader) throws Exception;

    }

}

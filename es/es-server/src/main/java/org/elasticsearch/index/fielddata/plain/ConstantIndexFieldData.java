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

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.AbstractSortedDocValues;
import org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;

public class ConstantIndexFieldData extends AbstractIndexOrdinalsFieldData {

    public static class Builder implements IndexFieldData.Builder {

        private final Function<MapperService, String> valueFunction;

        public Builder(Function<MapperService, String> valueFunction) {
            this.valueFunction = valueFunction;
        }

        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                CircuitBreakerService breakerService, MapperService mapperService) {
            return new ConstantIndexFieldData(indexSettings, fieldType.name(), valueFunction.apply(mapperService));
        }

    }

    private static class ConstantAtomicFieldData extends AbstractAtomicOrdinalsFieldData {

        private final String value;

        ConstantAtomicFieldData(String value) {
            super(DEFAULT_SCRIPT_FUNCTION);
            this.value = value;
        }


        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            final BytesRef term = new BytesRef(value);
            final SortedDocValues sortedValues = new AbstractSortedDocValues() {

                private int docID = -1;

                @Override
                public BytesRef lookupOrd(int ord) {
                    return term;
                }

                @Override
                public int getValueCount() {
                    return 1;
                }

                @Override
                public int ordValue() {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    docID = target;
                    return true;
                }

                @Override
                public int docID() {
                    return docID;
                }
            };
            return (SortedSetDocValues) DocValues.singleton(sortedValues);
        }

        @Override
        public void close() {
        }

    }

    private final ConstantAtomicFieldData atomicFieldData;

    private ConstantIndexFieldData(IndexSettings indexSettings, String name, String value) {
        super(indexSettings, name, null, null,
                TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
                TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
                TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE);
        atomicFieldData = new ConstantAtomicFieldData(value);
    }

    @Override
    public void clear() {
    }

    @Override
    public final AtomicOrdinalsFieldData load(LeafReaderContext context) {
        return atomicFieldData;
    }

    @Override
    public AtomicOrdinalsFieldData loadDirect(LeafReaderContext context)
            throws Exception {
        return atomicFieldData;
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested,
            boolean reverse) {
        final XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(DirectoryReader indexReader) throws Exception {
        return loadGlobal(indexReader);
    }

    public String getValue() {
        return atomicFieldData.value;
    }

}

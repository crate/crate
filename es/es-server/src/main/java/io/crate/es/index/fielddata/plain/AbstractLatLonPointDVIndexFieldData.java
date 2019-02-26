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
package io.crate.es.index.fielddata.plain;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import io.crate.es.common.Nullable;
import io.crate.es.index.Index;
import io.crate.es.index.IndexSettings;
import io.crate.es.index.fielddata.AtomicGeoPointFieldData;
import io.crate.es.index.fielddata.IndexFieldData;
import io.crate.es.index.fielddata.IndexFieldDataCache;
import io.crate.es.index.fielddata.IndexGeoPointFieldData;
import io.crate.es.index.mapper.MappedFieldType;
import io.crate.es.index.mapper.MapperService;
import io.crate.es.indices.breaker.CircuitBreakerService;
import io.crate.es.search.MultiValueMode;

public abstract class AbstractLatLonPointDVIndexFieldData extends DocValuesIndexFieldData
    implements IndexGeoPointFieldData {
    AbstractLatLonPointDVIndexFieldData(Index index, String fieldName) {
        super(index, fieldName);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, boolean reverse) {
        throw new IllegalArgumentException("can't sort on geo_point field without using specific sorting feature, like geo_distance");
    }

    public static class LatLonPointDVIndexFieldData extends AbstractLatLonPointDVIndexFieldData {
        public LatLonPointDVIndexFieldData(Index index, String fieldName) {
            super(index, fieldName);
        }

        @Override
        public AtomicGeoPointFieldData load(LeafReaderContext context) {
            LeafReader reader = context.reader();
            FieldInfo info = reader.getFieldInfos().fieldInfo(fieldName);
            if (info != null) {
                checkCompatible(info);
            }
            return new LatLonPointDVAtomicFieldData(reader, fieldName);
        }

        @Override
        public AtomicGeoPointFieldData loadDirect(LeafReaderContext context) throws Exception {
            return load(context);
        }

        /** helper: checks a fieldinfo and throws exception if its definitely not a LatLonDocValuesField */
        static void checkCompatible(FieldInfo fieldInfo) {
            // dv properties could be "unset", if you e.g. used only StoredField with this same name in the segment.
            if (fieldInfo.getDocValuesType() != DocValuesType.NONE
                && fieldInfo.getDocValuesType() != LatLonDocValuesField.TYPE.docValuesType()) {
                throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" was indexed with docValuesType="
                    + fieldInfo.getDocValuesType() + " but this type has docValuesType="
                    + LatLonDocValuesField.TYPE.docValuesType() + ", is the field really a LatLonDocValuesField?");
            }
        }
    }

    public static class Builder implements IndexFieldData.Builder {
        @Override
        public IndexFieldData<?> build(IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache,
                                       CircuitBreakerService breakerService, MapperService mapperService) {
            // ignore breaker
            return new LatLonPointDVIndexFieldData(indexSettings.getIndex(), fieldType.name());
        }
    }
}

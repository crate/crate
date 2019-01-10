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
package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;

import java.util.function.Function;

public class DocLookup {

    private final MapperService mapperService;
    private final Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup;

    @Nullable
    private final String[] types;

    DocLookup(MapperService mapperService, Function<MappedFieldType, IndexFieldData<?>> fieldDataLookup, @Nullable String[] types) {
        this.mapperService = mapperService;
        this.fieldDataLookup = fieldDataLookup;
        this.types = types;
    }

    public MapperService mapperService() {
        return this.mapperService;
    }

    public IndexFieldData<?> getForField(MappedFieldType fieldType) {
        return fieldDataLookup.apply(fieldType);
    }

    public LeafDocLookup getLeafDocLookup(LeafReaderContext context) {
        return new LeafDocLookup(mapperService, fieldDataLookup, types, context);
    }

    public String[] getTypes() {
        return types;
    }
}

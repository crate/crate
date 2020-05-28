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

import java.io.Closeable;
import java.io.IOException;

import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

public class IndexFieldDataService extends AbstractIndexComponent implements Closeable {

    private final CircuitBreakerService circuitBreakerService;
    private final MapperService mapperService;

    public IndexFieldDataService(IndexSettings indexSettings,
                                 CircuitBreakerService circuitBreakerService,
                                 MapperService mapperService) {
        super(indexSettings);
        this.circuitBreakerService = circuitBreakerService;
        this.mapperService = mapperService;
    }


    public IndexFieldData getForField(MappedFieldType fieldType) {
        return getForField(fieldType, index().getName());
    }

    public IndexFieldData getForField(MappedFieldType fieldType, String fullyQualifiedIndexName) {
        IndexFieldData.Builder builder = fieldType.fielddataBuilder(fullyQualifiedIndexName);
        return builder.build(indexSettings, fieldType, circuitBreakerService, mapperService);
    }

    @Override
    public void close() throws IOException {
    }
}

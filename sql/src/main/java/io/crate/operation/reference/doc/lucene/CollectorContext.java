/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.reference.doc.lucene;

import io.crate.operation.collect.collectors.CollectorFieldsVisitor;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.lookup.SourceLookup;

public class CollectorContext {

    private final MapperService mapperService;
    private final IndexFieldDataService fieldData;
    private final CollectorFieldsVisitor fieldsVisitor;
    private final int readerId;
    private final byte relationId;

    private SourceLookup sourceLookup;

    public CollectorContext(MapperService mapperService,
                            IndexFieldDataService fieldData,
                            CollectorFieldsVisitor visitor) {
        this(mapperService, fieldData, visitor, -1, (byte) 0);
    }

    public CollectorContext(MapperService mapperService,
                            IndexFieldDataService fieldData,
                            CollectorFieldsVisitor visitor,
                            int readerId,
                            byte relationId) {
        this.mapperService = mapperService;
        this.fieldData = fieldData;
        fieldsVisitor = visitor;
        this.readerId = readerId;
        this.relationId = relationId;
    }

    public CollectorFieldsVisitor visitor() {
        return fieldsVisitor;
    }

    public int readerId() {
        return readerId;
    }

    public byte relationId() {
        return relationId;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public IndexFieldDataService fieldData() {
        return fieldData;
    }

    public SourceLookup sourceLookup() {
        if (sourceLookup == null) {
            sourceLookup = new SourceLookup();
        }
        return sourceLookup;
    }
}

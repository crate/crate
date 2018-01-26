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

package io.crate.expression.reference.doc.lucene;

import io.crate.execution.engine.collect.collectors.CollectorFieldsVisitor;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.search.lookup.SourceLookup;

public class CollectorContext {

    private final IndexFieldDataService fieldData;
    private final CollectorFieldsVisitor fieldsVisitor;
    private final int jobSearchContextId;

    private SourceLookup sourceLookup;

    public CollectorContext(IndexFieldDataService fieldData,
                            CollectorFieldsVisitor visitor) {
        this(fieldData, visitor, -1);
    }

    public CollectorContext(IndexFieldDataService fieldData,
                            CollectorFieldsVisitor visitor,
                            int jobSearchContextId) {
        this.fieldData = fieldData;
        fieldsVisitor = visitor;
        this.jobSearchContextId = jobSearchContextId;
    }

    public CollectorFieldsVisitor visitor() {
        return fieldsVisitor;
    }

    public int jobSearchContextId() {
        return jobSearchContextId;
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

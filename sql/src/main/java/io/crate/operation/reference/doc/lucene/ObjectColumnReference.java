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


import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SourceLookup;

import java.util.Map;

public class ObjectColumnReference extends ColumnReferenceCollectorExpression<Map<String, Object>> {

    protected SourceLookup sourceLookup;

    public ObjectColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public void setNextDocId(int doc) {
        // TODO: FIX ME! setNextDocId not available
        // sourceLookup.setNextDocId(doc);
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        // TODO: FIX ME! setNextReader not available
        // sourceLookup.setNextReader(context);
    }

    @Override
    public void startCollect(CollectorContext context) {
        sourceLookup = context.sourceLookup();
    }


    @Override
    public Map<String, Object> value() {
        return (Map<String, Object>)sourceLookup.extractValue(columnName);
    }
}

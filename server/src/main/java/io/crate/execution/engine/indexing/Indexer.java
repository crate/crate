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

package io.crate.execution.engine.indexing;

import java.util.Collection;
import java.util.List;

import org.apache.lucene.document.Field;
import org.elasticsearch.index.mapper.ParseContext.Document;

import io.crate.common.collections.Lists2;
import io.crate.metadata.Reference;

public class Indexer {

    private final List<ValueIndexer<?>> indexers;

    public Indexer(Collection<? extends Reference> columns) {
        this.indexers = Lists2.map(
            columns,
            x -> x.valueType().valueIndexer(x.column().fqn()));
    }

    @SuppressWarnings("unchecked")
    public Document createDoc(Object[] values) {
        var doc = new Document();
        for (int i = 0; i < values.length; i++) {
            var indexer = (ValueIndexer<Object>) indexers.get(i);
            List<Field> indexValue = indexer.indexValue(values[i]);
            for (Field field : indexValue) {
                doc.add(field);
            }
        }
        return doc;
    }
}

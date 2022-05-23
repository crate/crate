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
import java.util.function.Consumer;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.mapper.Uid;

import io.crate.common.collections.Lists2;
import io.crate.metadata.Reference;

public class Indexer {

    private final List<ValueIndexer<?>> indexers;

    public Indexer(Collection<? extends Reference> columns) {
        this.indexers = Lists2.map(
            columns,
            x -> x.valueType().valueIndexer(x.column().fqn()));
    }

    // TODO: Need to handle system columns (_field_names, _id, _seq_no, _source(?), _version)

    @SuppressWarnings("unchecked")
    public Document createDoc(String id, Object[] values, BytesReference source) {
        var doc = new Document();
        Consumer<? super Field> addField = doc::add;
        for (int i = 0; i < values.length; i++) {
            var indexer = (ValueIndexer<Object>) indexers.get(i);
            indexer.indexValue(values[i], addField);
        }
        BytesRef idBytes = Uid.encodeId(id);
        doc.add(new Field("_id", idBytes, IdFieldMapper.Defaults.FIELD_TYPE));
        BytesRef ref = source.toBytesRef();
        doc.add(new StoredField("_source", ref.bytes, ref.offset, ref.length));
        return doc;
    }
}

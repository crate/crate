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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.KeywordFieldMapper;

public class StringValueIndexer implements ValueIndexer<String> {

    private final String name;

    public StringValueIndexer(String name) {
        this.name = name;
    }

    @Override
    public List<Field> indexValue(String value) {
        ArrayList<Field> fields = new ArrayList<>();
        final BytesRef binaryValue = new BytesRef(value);
        fields.add(new Field(name, binaryValue, KeywordFieldMapper.Defaults.FIELD_TYPE));
        fields.add(new SortedDocValuesField(name, binaryValue));
        return fields;
    }
}

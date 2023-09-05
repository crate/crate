/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dml;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

public class StringIndexer implements ValueIndexer<String> {

    private final Reference ref;
    private final FieldType fieldType;

    public StringIndexer(Reference ref, @Nullable FieldType fieldType) {
        this.ref = ref;
        this.fieldType = fieldType == null ? KeywordFieldMapper.Defaults.FIELD_TYPE : fieldType;
    }

    @Override
    public void indexValue(String value,
                           XContentBuilder xcontentBuilder,
                           Consumer<? super IndexableField> addField,
                           Map<ColumnIdent, Indexer.Synthetic> synthetics,
                           Map<ColumnIdent, Indexer.ColumnConstraint> toValidate,
                           Function<Reference, String> columnKeyProvider) throws IOException {
        xcontentBuilder.value(value);
        String name = ref.column().fqn();
        BytesRef binaryValue = new BytesRef(value);
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(name, binaryValue, fieldType);
            addField.accept(field);
            if (ref.hasDocValues() == false && fieldType.omitNorms()) {
                addField.accept(new Field(
                    FieldNamesFieldMapper.NAME,
                    name,
                    FieldNamesFieldMapper.Defaults.FIELD_TYPE));
            }
        }
        if (ref.hasDocValues()) {
            addField.accept(new SortedSetDocValuesField(name, binaryValue));
        }
    }
}

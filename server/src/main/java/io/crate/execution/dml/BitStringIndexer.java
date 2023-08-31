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
import java.util.BitSet;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.BitStringFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.execution.dml.Indexer.Synthetic;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.sql.tree.BitString;

public class BitStringIndexer implements ValueIndexer<BitString> {

    private final Reference ref;
    private final FieldType fieldType;
    private final String name;

    public BitStringIndexer(Reference ref, FieldType fieldType) {
        this.ref = ref;
        this.name = ref.column().fqn();
        this.fieldType = fieldType == null
            ? BitStringFieldMapper.Defaults.FIELD_TYPE
            : fieldType;
    }

    @Override
    public void indexValue(BitString value,
                           XContentBuilder xcontentBuilder,
                           Consumer<? super IndexableField> addField,
                           Map<ColumnIdent, Synthetic> synthetics,
                           Map<ColumnIdent, ColumnConstraint> toValidate,
                           Function<Reference, String> columnKeyProvider) throws IOException {
        BitSet bitSet = value.bitSet();
        byte[] bytes = bitSet.toByteArray();
        xcontentBuilder.value(bytes);

        BytesRef binaryValue = new BytesRef(bytes);
        if (ref.indexType() != IndexType.NONE) {
            addField.accept(new Field(name, binaryValue, fieldType));
        }

        if (ref.hasDocValues()) {
            addField.accept(new SortedSetDocValuesField(name, binaryValue));
        } else {
            addField.accept(new Field(
                FieldNamesFieldMapper.NAME,
                name,
                FieldNamesFieldMapper.Defaults.FIELD_TYPE));
        }
    }
}

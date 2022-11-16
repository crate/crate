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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;

import io.crate.common.collections.Lists2;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;

public class Indexer {

    private final List<ValueIndexer<?>> valueIndexers;
    private final List<Reference> columns;

    public Indexer(DocTableInfo table, List<Reference> targetColumns) {
        this.columns = targetColumns;
        this.valueIndexers = Lists2.map(
            targetColumns,
            ref -> ref.valueType().valueIndexer(table.ident(), ref, table::getReference));
    }

    public ParsedDocument index(Object[] values) throws IOException {
        assert values.length == valueIndexers.size()
            : "Number of values must match number of targetColumns/valueIndexers";

        Document document = new Document();
        Consumer<? super IndexableField> addField = document::add;
        ArrayList<Reference> newColumns = new ArrayList<>();
        Consumer<? super Reference> onDynamicColumn = newColumns::add;
        // TODO: re-use stream?
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        for (int i = 0; i < values.length; i++) {
            Reference reference = columns.get(i);
            Object value = values[i];
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) valueIndexers.get(i);
            xContentBuilder.field(reference.column().leafName());
            valueIndexer.indexValue(value, xContentBuilder, addField, onDynamicColumn);
        }
        xContentBuilder.endObject();

        NumericDocValuesField version = new NumericDocValuesField(DocSysColumns.Names.VERSION, -1L);
        document.add(version);

        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        return new ParsedDocument(
            version,
            seqID,
            null,
            document,
            BytesReference.bytes(xContentBuilder),
            null,
            newColumns
        );
    }
}

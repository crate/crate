/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.Uid;

import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;

/**
 * Parses transaction log entries based on column information from a {@link DocTableInfo}
 */
public class TranslogIndexer {

    private record ColumnIndexer<T>(DataType<T> dataType, ValueIndexer<Object> valueIndexer) {}

    private final Map<String, ColumnIndexer<?>> indexers = new HashMap<>();
    private final Map<String, List<String>> tableIndexSources = new HashMap<>();
    private final boolean ignoreUnknownColumns;
    private final SourceParser sourceParser;

    /**
     * Creates a new TranslogIndexer backed by a DocTableInfo instance
     */
    @SuppressWarnings("unchecked")
    public TranslogIndexer(DocTableInfo table) {
        for (var ref : table.columns()) {
            var storageSupport = ref.valueType().storageSupport();
            if (storageSupport != null) {
                var columnIndexer = new ColumnIndexer<>(
                    ref.valueType(),
                    (ValueIndexer<Object>) storageSupport.valueIndexer(table.ident(), ref, table::getReference));
                indexers.put(ref.column().name(), columnIndexer);
            }
        }
        for (var ref : table.indexColumns()) {
            for (var source : ref.columns()) {
                tableIndexSources.computeIfAbsent(source.column().fqn(), _ -> new ArrayList<>()).add(ref.storageIdent());
            }
        }
        this.ignoreUnknownColumns = table.columnPolicy() != ColumnPolicy.STRICT;
        this.sourceParser = new SourceParser(table);
    }

    /**
     * Convert a transaction log entry to a ParsedDocument to be indexed
     * @param id        the document ID
     * @param source    the transaction log entry bytes
     */
    public ParsedDocument index(String id, BytesReference source) {

        Document doc = new Document();

        var stream = new BytesStreamOutput();
        try (XContentBuilder xContentBuilder = XContentFactory.json(stream)) {

            populateLuceneFields(source, doc, xContentBuilder);

            NumericDocValuesField version = new NumericDocValuesField(DocSysColumns.Names.VERSION, -1L);
            doc.add(version);

            doc.add(new StoredField("_source", source.toBytesRef()));

            BytesRef idBytes = Uid.encodeId(id);
            doc.add(new Field(DocSysColumns.Names.ID, idBytes, IdFieldMapper.Defaults.FIELD_TYPE));

            SequenceIDFields seqID = SequenceIDFields.emptySeqID();
            // Actual values are set via ParsedDocument.updateSeqID
            doc.add(seqID.seqNo);
            doc.add(seqID.seqNoDocValue);
            doc.add(seqID.primaryTerm);

            return new ParsedDocument(
                version,
                seqID,
                id,
                doc,
                source
            );

        } catch (IOException | UncheckedIOException e) {
            throw new MapperParsingException("Error parsing translog source", e);
        }

    }

    private void populateLuceneFields(BytesReference source, Document doc, XContentBuilder xcontent) throws IOException {
        Map<String, Object> docMap = sourceParser.parse(source, ignoreUnknownColumns);

        for (var entry : docMap.entrySet()) {
            var column = entry.getKey();
            var indexer = indexers.get(column);
            if (indexer == null) {
                if (isEmpty(entry.getValue())) {
                    continue;
                }
                throw new TranslogMappingUpdateException();
            }

            Object castValue = valueForInsert(indexer.dataType, entry.getValue());
            if (castValue != null) {
                indexer.valueIndexer.indexValue(castValue, xcontent, doc::add, Map.of(), Map.of());
                if (tableIndexSources.containsKey(column)) {
                    addIndexField(doc, tableIndexSources.get(column), castValue);
                }
            }
        }
    }

    private static boolean isEmpty(Object value) {
        switch (value) {
            case null -> {
                return true;
            }
            case List<?> l -> {
                if (l.isEmpty())
                    return true;
                return l.stream().allMatch(TranslogIndexer::isEmpty);
            }
            case Map<?, ?> m -> {
                for (var entry : m.entrySet()) {
                    if (isEmpty(entry.getValue()) == false) {
                        return false;
                    }
                }
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private static <T> T valueForInsert(DataType<T> valueType, Object value) {
        return valueType.valueForInsert(valueType.sanitizeValue(value));
    }

    private static void addIndexField(Document doc, List<String> targetFields, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof Iterable<?> it) {
            for (Object val : it) {
                if (val == null) {
                    continue;
                }
                targetFields.forEach(field -> addIndexField(doc, field, val));
            }
        } else {
            targetFields.forEach(field -> addIndexField(doc, field, value));
        }
    }

    private static void addIndexField(Document doc, String field, Object value) {
        doc.add(new Field(field, value.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
    }

}

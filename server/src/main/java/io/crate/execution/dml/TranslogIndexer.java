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

import org.apache.lucene.document.Field;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;

import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;

/**
 * Parses transaction log entries based on column information from a {@link DocTableInfo}
 */
public class TranslogIndexer {

    private record ColumnIndexer<T>(DataType<T> dataType, ValueIndexer<Object> valueIndexer) {
    }

    private final Map<String, ColumnIndexer<?>> indexers = new HashMap<>();
    private final Map<String, List<String>> tableIndexSources = new HashMap<>();
    private final boolean ignoreUnknownColumns;
    private final SourceParser sourceParser;
    private final Version tableVersionCreated;

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
        this.tableVersionCreated = table.versionCreated();
    }

    /**
     * Convert a transaction log entry to a ParsedDocument to be indexed
     *
     * @param id     the document ID
     * @param source the transaction log entry bytes
     */
    public ParsedDocument index(String id, BytesReference source) {
        try {
            return populateLuceneFields(source).build(id);
        } catch (IOException | UncheckedIOException e) {
            throw new MapperParsingException("Error parsing translog source", e);
        }

    }

    private IndexDocumentBuilder populateLuceneFields(BytesReference source) throws IOException {
        Map<String, Object> docMap = sourceParser.parse(source, ignoreUnknownColumns == false);
        IndexDocumentBuilder docBuilder = new IndexDocumentBuilder(TranslogWriter.wrapBytes(source), _ -> null, Map.of(), tableVersionCreated);
        for (var entry : docMap.entrySet()) {
            var column = entry.getKey();
            var indexer = indexers.get(column);
            if (indexer == null) {
                if (isEmpty(entry.getValue())) {
                    continue;
                }
                throw new TranslogMappingUpdateException("Unknown column in translog entry: " + entry);
            }

            Object castValue = valueForInsert(indexer.dataType, entry.getValue());
            if (castValue != null) {
                indexer.valueIndexer.indexValue(castValue, docBuilder);
                if (tableIndexSources.containsKey(column)) {
                    addIndexField(docBuilder, tableIndexSources.get(column), castValue);
                }
            }
        }
        return docBuilder;
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

    private static void addIndexField(IndexDocumentBuilder docBuilder, List<String> targetFields, Object value) {
        if (value == null) {
            return;
        }
        if (value instanceof Iterable<?> it) {
            for (Object val : it) {
                if (val == null) {
                    continue;
                }
                targetFields.forEach(field -> addIndexField(docBuilder, field, val));
            }
        } else {
            targetFields.forEach(field -> addIndexField(docBuilder, field, value));
        }
    }

    private static void addIndexField(IndexDocumentBuilder docBuilder, String field, Object value) {
        docBuilder.addField(new Field(field, value.toString(), FulltextIndexer.FIELD_TYPE));
    }

}

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

import static io.crate.execution.dml.Indexer.addIndexColumns;
import static io.crate.execution.dml.Indexer.buildIndexColumns;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;

import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.metadata.DocReferences;
import io.crate.metadata.RowGranularity;
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
    private final boolean ignoreUnknownColumns;
    private final SourceParser sourceParser;
    private final Version shardCreatedVersion;

    private final List<Indexer.IndexColumn> indexColumns;

    /**
     * Creates a new TranslogIndexer backed by a DocTableInfo instance
     */
    @SuppressWarnings("unchecked")
    public TranslogIndexer(DocTableInfo table, Version shardCreatedVersion) {
        sourceParser = new SourceParser(table.droppedColumns(), table.lookupNameBySourceKey(), false);
        for (var ref : table.columns()) {
            var storageSupport = ref.valueType().storageSupport();
            if (storageSupport != null) {
                var columnIndexer = new ColumnIndexer<>(
                    ref.valueType(),
                    (ValueIndexer<Object>) storageSupport.valueIndexer(table.ident(), ref, table::getReference));
                indexers.put(ref.column().name(), columnIndexer);
            }
            if (ref.granularity() == RowGranularity.DOC) {
                sourceParser.register(DocReferences.toDocLookup(ref).column(), ref.valueType());
            }
        }

        this.indexColumns = buildIndexColumns(
            table.indexColumns(),
            table::getReference,
            reference -> {
                List<String> fullPath = new ArrayList<>(reference.column().path().size() + 1);
                fullPath.add(reference.column().name());
                fullPath.addAll(reference.column().path());
                return new TranslogInput(fullPath);
            }
        );
        ignoreUnknownColumns = table.columnPolicy() != ColumnPolicy.STRICT;
        this.shardCreatedVersion = shardCreatedVersion;
    }


    private static class TranslogInput implements Input<Object> {

        private final List<String> fullPath;
        private Map<String, Object> docMap;

        TranslogInput(List<String> fullPath) {
            this.fullPath = fullPath;
        }

        public void setTranslogEntry(Map<String, Object> docMap) {
            this.docMap = docMap;
        }

        @Override
        public Object value() {
            return Maps.getByPath(docMap, fullPath);
        }
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
        IndexDocumentBuilder docBuilder = new IndexDocumentBuilder(TranslogWriter.wrapBytes(source), _ -> null, Map.of(), shardCreatedVersion);
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
            }
            // Make sanitized value visible for the TranslogInputs.
            // Fulltext fields must use sanitized values.
            // NULL values are skipped in the addIndexColumns.
            entry.setValue(castValue);
        }

        for (Indexer.IndexColumn indexColumn: indexColumns) {
            for (Input<?> input: indexColumn.inputs()) {
                TranslogInput translogInput = (TranslogInput) input;
                translogInput.setTranslogEntry(docMap);
            }
        }
        addIndexColumns(indexColumns, docBuilder);

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
        // Mapping could have changed since the translog entry was written, so we need to sanitize any column
        // leniently (use NULL on sanitization errors) to avoid failing the index operation.
        return valueType.valueForInsert(valueType.sanitizeValueLenient(value));
    }

}

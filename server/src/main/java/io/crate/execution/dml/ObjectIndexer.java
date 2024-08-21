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

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.data.Input;
import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;

public class ObjectIndexer implements ValueIndexer<Map<String, Object>> {

    private final ColumnIdent column;
    private final Map<String, Child> children = new HashMap<>();
    private final Function<ColumnIdent, Reference> getRef;
    private final RelationName table;
    private final Reference ref;
    private final String unknownColumnPrefix;
    private final Streamer<Map<String, Object>> streamer;

    private record Child(Reference reference, ValueIndexer<Object> indexer) {
        ColumnIdent ident() {
            return reference.column();
        }
    }

    @SuppressWarnings("unchecked")
    public ObjectIndexer(RelationName table,
                         Reference ref,
                         Function<ColumnIdent, Reference> getRef,
                         Streamer<Map<String, Object>> streamer) {
        this.table = table;
        this.ref = ref;
        this.getRef = getRef;
        this.streamer = streamer;
        this.unknownColumnPrefix = ref.oid() != COLUMN_OID_UNASSIGNED ? SourceParser.UNKNOWN_COLUMN_PREFIX : "";
        this.column = ref.column();
        ObjectType objectType = (ObjectType) ArrayType.unnest(ref.valueType());
        for (var entry : objectType.innerTypes().entrySet()) {
            String innerName = entry.getKey();
            DataType<?> value = entry.getValue();
            ColumnIdent child = column.getChild(innerName);
            Reference childRef = getRef.apply(child);
            if (childRef == null) {
                // Race, either column got deleted or stale DocTableInfo?
                // Treat it as dynamic column if a value for the nested column is found
                continue;
            }
            ValueIndexer<?> indexer
                = childRef.granularity() == RowGranularity.PARTITION ? null : value.valueIndexer(table, childRef, getRef);
            children.put(innerName, new Child(childRef, (ValueIndexer<Object>) indexer));
        }
    }

    @Override
    public void indexValue(@NotNull Map<String, Object> value, IndexDocumentBuilder docBuilder) throws IOException {
        TranslogWriter translogWriter = docBuilder.translogWriter();
        translogWriter.startObject();
        for (var entry : children.entrySet()) {
            String innerName = entry.getKey();
            Child child = entry.getValue();
            if (value.containsKey(innerName) == false) {
                var synth = docBuilder.getSyntheticValue(child.ident());
                if (synth != null) {
                    // directly modify the map so that containing types will see the value
                    // if they need to write stored fields
                    value.put(innerName, synth);
                }
            }
            var innerValue = value.get(innerName);
            docBuilder.checkColumnConstraint(child.ident(), innerValue);
            if (innerValue == null) {
                continue;
            }
            var valueIndexer = child.indexer;
            // valueIndexer is null for partitioned columns
            if (valueIndexer != null) {
                docBuilder.translogWriter().writeFieldName(child.reference.storageIdentLeafName());
                valueIndexer.indexValue(
                    child.reference.valueType().sanitizeValue(innerValue),
                    docBuilder
                );
            }
        }
        Map<String, Object> ignoredColumns = new HashMap<>();
        value.forEach((k, v) -> {
            if (children.containsKey(k) == false) {
                translogWriter.writeFieldName(this.unknownColumnPrefix + k);
                translogWriter.writeValue(v);
                ignoredColumns.put(k, v);
            }
        });
        if (docBuilder.maybeAddStoredField()) {
            if (ignoredColumns.isEmpty() == false) {
                docBuilder.addField(new StoredField(ref.storageIdentLeafName(), toBytes(ignoredColumns).toBytesRef()));
            } else if (value.isEmpty()) {
                docBuilder.addField(new StoredField(ref.storageIdentLeafName(), new BytesRef("{}")));
            }
        }
        translogWriter.endObject();
    }

    private BytesReference toBytes(Map<String, Object> map) {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            streamer.writeValueTo(output, map);
            return output.bytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void collectSchemaUpdates(@Nullable Map<String, Object> value,
                                     Consumer<? super Reference> onDynamicColumn,
                                     Synthetics synthetics) throws IOException {
        for (var entry : children.entrySet()) {
            String innerName = entry.getKey();
            Child child = entry.getValue();
            Object innerValue = null;
            if (value == null || value.containsKey(innerName) == false) {
                Input<Object> synthetic = synthetics.get(child.ident());
                if (synthetic != null) {
                    innerValue = synthetic.value();
                }
            } else {
                innerValue = value.get(innerName);
            }
            // valueIndexer is null for partitioned columns
            if (child.indexer != null) {
                child.indexer.collectSchemaUpdates(
                    child.reference.valueType().sanitizeValue(innerValue),
                    onDynamicColumn,
                    synthetics
                );
            }
        }
        if (value != null) {
            addNewColumns(value, onDynamicColumn, synthetics);
        }
    }

    @Override
    public void updateTargets(Function<ColumnIdent, Reference> getRef) {
        for (var entry : children.entrySet()) {
            var newChildRef = getRef.apply(entry.getValue().ident());
            if (Objects.equals(entry.getValue().reference, newChildRef) == false) {
                // noinspection unchecked
                ValueIndexer<Object> indexer = newChildRef.granularity() == RowGranularity.PARTITION
                    ? null : (ValueIndexer<Object>) newChildRef.valueType().valueIndexer(newChildRef.ident().tableIdent(), newChildRef, getRef);
                children.put(entry.getKey(), new Child(newChildRef, indexer));
            }
            entry.getValue().indexer.updateTargets(getRef);
        }
    }

    @Override
    public String storageIdentLeafName() {
        return ref.storageIdentLeafName();
    }

    @SuppressWarnings("unchecked")
    private void addNewColumns(Map<String, Object> value,
                               Consumer<? super Reference> onDynamicColumn,
                               Synthetics synthetics) throws IOException {
        int position = -1;
        for (var entry : value.entrySet()) {
            String innerName = entry.getKey();
            Object innerValue = entry.getValue();
            if (children.containsKey(innerName) || innerValue == null) {
                continue;
            }
            if (ref.columnPolicy() == ColumnPolicy.STRICT) {
                throw new IllegalArgumentException(String.format(
                    Locale.ENGLISH,
                    "Cannot add column `%s` to strict object `%s`",
                    innerName,
                    ref.column()
                ));
            }
            if (ref.columnPolicy() == ColumnPolicy.IGNORED) {
                continue;
            }
            var type = DynamicIndexer.guessType(innerValue);
            DynamicIndexer.throwOnNestedArray(type);
            innerValue = type.sanitizeValue(innerValue);
            StorageSupport<?> storageSupport = type.storageSupport();
            if (storageSupport == null) {
                if (DynamicIndexer.handleEmptyArray(type, innerValue)) {
                    continue;
                }
                throw new IllegalArgumentException(
                    "Cannot create columns of type " + type.getName() + " dynamically. " +
                        "Storage is not supported for this type");
            }
            boolean nullable = true;
            Symbol defaultExpression = null;
            Reference newColumn = new SimpleReference(
                new ReferenceIdent(table, column.getChild(innerName)),
                RowGranularity.DOC,
                type,
                ref.columnPolicy(),
                IndexType.PLAIN,
                nullable,
                storageSupport.docValuesDefault(),
                position,
                COLUMN_OID_UNASSIGNED,
                false,
                defaultExpression
            );
            position--;
            onDynamicColumn.accept(newColumn);
            var valueIndexer = (ValueIndexer<Object>) type.valueIndexer(
                table,
                newColumn,
                getRef
            );
            children.put(innerName, new Child(newColumn, valueIndexer));
            valueIndexer.collectSchemaUpdates(
                innerValue,
                onDynamicColumn,
                synthetics
            );
        }
    }
}

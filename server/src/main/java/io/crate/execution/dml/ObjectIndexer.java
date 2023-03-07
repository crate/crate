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
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.execution.dml.Indexer.Synthetic;
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

    private final ObjectType objectType;
    private final HashMap<String, ValueIndexer<Object>> innerIndexers;
    private final ColumnIdent column;
    private final Function<ColumnIdent, Reference> getRef;
    private final RelationName table;
    private final Reference ref;
    private final Function<ColumnIdent, FieldType> getFieldType;
    private final HashMap<String, DataType<?>> innerTypes;

    @SuppressWarnings("unchecked")
    public ObjectIndexer(RelationName table,
                         Reference ref,
                         Function<ColumnIdent, FieldType> getFieldType,
                         Function<ColumnIdent, Reference> getRef) {
        this.table = table;
        this.ref = ref;
        this.getFieldType = getFieldType;
        this.getRef = getRef;
        this.column = ref.column();
        this.objectType = (ObjectType) ArrayType.unnest(ref.valueType());
        this.innerIndexers = new HashMap<>();
        this.innerTypes = new HashMap<>(objectType.innerTypes());
        for (var entry : objectType.innerTypes().entrySet()) {
            String innerName = entry.getKey();
            DataType<?> value = entry.getValue();
            ColumnIdent child = column.getChild(innerName);
            Reference childRef = getRef.apply(child);
            if (childRef == null) {
                // Race, either column got deleted or stale DocTableInfo?
                // Treat it as dynamic column if a value for the nested column is found
                innerTypes.remove(innerName);
            } else if (childRef.granularity() != RowGranularity.PARTITION) {
                ValueIndexer<?> valueIndexer = value.valueIndexer(
                    table,
                    childRef,
                    getFieldType,
                    getRef
                );
                innerIndexers.put(entry.getKey(), (ValueIndexer<Object>) valueIndexer);
            }
        }
    }

    @Override
    public void indexValue(@Nullable Map<String, Object> value,
                           XContentBuilder xContentBuilder,
                           Consumer<? super IndexableField> addField,
                           Consumer<? super Reference> onDynamicColumn,
                           Map<ColumnIdent, Indexer.Synthetic> synthetics,
                           Map<ColumnIdent, Indexer.ColumnConstraint> checks) throws IOException {
        xContentBuilder.startObject();
        for (var entry : innerTypes.entrySet()) {
            String innerName = entry.getKey();
            DataType<?> type = entry.getValue();
            ColumnIdent innerColumn = column.getChild(innerName);
            Object innerValue = value == null ? null : value.get(innerName);
            if (innerValue == null) {
                Synthetic synthetic = synthetics.get(innerColumn);
                if (synthetic != null) {
                    innerValue = synthetic.input().value();
                }
            }
            ColumnConstraint check = checks.get(innerColumn);
            if (check != null) {
                check.verify(innerValue);
            }
            if (innerValue == null) {
                continue;
            }
            var valueIndexer = innerIndexers.get(innerName);
            // valueIndexer is null for partitioned columns
            if (valueIndexer != null) {
                xContentBuilder.field(innerName);
                valueIndexer.indexValue(
                    type.sanitizeValue(innerValue),
                    xContentBuilder,
                    addField,
                    onDynamicColumn,
                    synthetics,
                    checks
                );
            }
        }
        if (value != null) {
            addNewColumns(value, xContentBuilder, addField, onDynamicColumn, synthetics, checks);
        }
        xContentBuilder.endObject();
    }

    @SuppressWarnings("unchecked")
    private void addNewColumns(Map<String, Object> value,
                               XContentBuilder xContentBuilder,
                               Consumer<? super IndexableField> addField,
                               Consumer<? super Reference> onDynamicColumn,
                               Map<ColumnIdent, Indexer.Synthetic> synthetics,
                               Map<ColumnIdent, Indexer.ColumnConstraint> checks) throws IOException {
        int position = -1;
        for (var entry : value.entrySet()) {
            String innerName = entry.getKey();
            Object innerValue = entry.getValue();
            boolean isNewColumn = !innerTypes.containsKey(innerName);
            if (!isNewColumn) {
                continue;
            }
            if (innerValue == null) {
                xContentBuilder.nullField(innerName);
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
                xContentBuilder.field(innerName, innerValue);
                continue;
            }
            var type = DynamicIndexer.guessType(innerValue);
            innerValue = type.sanitizeValue(innerValue);
            StorageSupport<?> storageSupport = type.storageSupport();
            if (storageSupport == null) {
                xContentBuilder.field(innerName);
                if (DynamicIndexer.handleEmptyArray(type, innerValue, xContentBuilder)) {
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
                defaultExpression
            );
            position--;
            onDynamicColumn.accept(newColumn);
            var valueIndexer = (ValueIndexer<Object>) type.valueIndexer(
                table,
                newColumn,
                getFieldType,
                getRef
            );
            innerIndexers.put(innerName, valueIndexer);
            innerTypes.put(innerName, type);
            xContentBuilder.field(innerName);
            valueIndexer.indexValue(
                innerValue,
                xContentBuilder,
                addField,
                onDynamicColumn,
                synthetics,
                checks
            );
        }
    }
}

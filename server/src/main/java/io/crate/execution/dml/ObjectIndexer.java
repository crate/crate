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
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;

public class ObjectIndexer implements ValueIndexer<Map<String, Object>> {

    private final ObjectType objectType;
    private final HashMap<String, ValueIndexer<Object>> innerIndexers;
    private final ColumnIdent column;
    private final Function<ColumnIdent, Reference> getRef;
    private final RelationName table;
    private final Reference ref;

    public ObjectIndexer(RelationName table,
                         Reference ref,
                         Function<ColumnIdent, Reference> getRef) {
        this.table = table;
        this.ref = ref;
        this.getRef = getRef;
        this.column = ref.column();
        this.objectType = (ObjectType) ref.valueType();
        this.innerIndexers = new HashMap<>();
        for (var entry : objectType.innerTypes().entrySet()) {
            String innerName = entry.getKey();
            DataType<?> value = entry.getValue();
            ColumnIdent child = ColumnIdent.getChild(column, innerName);
            Reference childRef = getRef.apply(child);
            ValueIndexer<?> valueIndexer = value.valueIndexer(table, childRef, getRef);
            if (valueIndexer != null) {
                innerIndexers.put(entry.getKey(), (ValueIndexer<Object>) valueIndexer);
            }
        }
    }

    @Override
    public void indexValue(Map<String, Object> value,
                           XContentBuilder xContentBuilder,
                           Consumer<? super IndexableField> addField,
                           Consumer<? super Reference> onDynamicColumn) throws IOException {
        xContentBuilder.startObject();
        for (var entry : value.entrySet()) {
            String key = entry.getKey();
            Object innerValue = entry.getValue();
            ValueIndexer<Object> valueIndexer = innerIndexers.get(key);
            if (valueIndexer == null) {
                var type = DataTypes.guessType(innerValue);
                StorageSupport<?> storageSupport = type.storageSupport();
                if (storageSupport == null) {
                    throw new IllegalArgumentException(
                        "Cannot create columns of type " + type.getName() + " dynamically. " +
                        "Storage is not supported for this type");
                }
                boolean nullable = true;
                Symbol defaultExpression = null;
                Reference newColumn = new SimpleReference(
                    new ReferenceIdent(table, column.getChild(key)),
                    RowGranularity.DOC,
                    type,
                    ref.columnPolicy(),
                    IndexType.PLAIN,
                    nullable,
                    storageSupport.docValuesDefault(),
                    -1,
                    defaultExpression
                );
                onDynamicColumn.accept(newColumn);
                valueIndexer = (ValueIndexer<Object>) type.valueIndexer(table, newColumn, getRef);
                innerIndexers.put(key, valueIndexer);
            }

            xContentBuilder.field(key);
            valueIndexer.indexValue(innerValue, xContentBuilder, addField, onDynamicColumn);
        }
        xContentBuilder.endObject();
    }
}

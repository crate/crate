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

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jetbrains.annotations.NotNull;

import io.crate.common.collections.Maps;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public class ColumnFieldVisitor extends StoredFieldVisitor {

    private final Map<String, Field> fields = new HashMap<>();
    private final Set<ColumnIdent> droppedColumns;
    private final SourceParser arraySourceParser;

    public ColumnFieldVisitor(DocTableInfo table) {
        this.droppedColumns
            = table.droppedColumns().stream().map(Reference::column).collect(Collectors.toUnmodifiableSet());
        this.arraySourceParser = new SourceParser(table);
    }

    private interface Field extends Comparable<Field> {

        Object sanitize(Object v);

        ColumnIdent column();

        default int compareTo(@NotNull ColumnFieldVisitor.Field o) {
            return column().compareTo(o.column());
        }
    }

    private record ValueField(DataType<?> dataType, ColumnIdent column) implements Field {
        @Override
        public Object sanitize(Object v) {
            return dataType.sanitizeValue(v);
        }
    }

    private record ArrayOfObjectField(DataType<?> dataType, ObjectType objectType, ColumnIdent column, SourceParser sourceParser) implements Field {
        @Override
        public Object sanitize(Object v) {
            try {
                var map = sourceParser.parse(new BytesArray((byte[]) v), Map.of(column.leafName(), objectType.innerTypes()), false);
                if (map.isEmpty()) {
                    return List.of();
                }
                return map.values().iterator().next();
            } catch (NotXContentException e) {
                // may be an array of nulls inserted before the field was upcast to an array of objects
                try (StreamInput in = new ByteBufferStreamInput(ByteBuffer.wrap((byte[]) v))) {
                    return in.readList(StreamInput::readGenericValue);
                } catch (IOException io) {
                    throw new UncheckedIOException(io);
                }
            }
        }
    }

    // Maps.mergeInto() needs its inputs to be sorted, to ensure that a parent object o doesn't overwrite
    // an already written child o['child'], so we read stored fields into a sorted map and then
    // iterate them in column order when converting them into a docMap.
    private Map<Field, Object> doc = new TreeMap<>();

    public void registerRef(Reference ref) {
        if (droppedColumns.contains(ref.column())) {
            return;
        }
        var column = ref.column();
        if (column.name().equals(SysColumns.Names.DOC)) {
            column = column.shiftRight();
        }
        var storageName = ref.storageIdentLeafName();
        if (ref.valueType() instanceof ArrayType<?>) {
            storageName = ArrayIndexer.ARRAY_VALUES_FIELD_PREFIX + storageName;
            if (ArrayType.unnest(ref.valueType()) instanceof ObjectType o) {
                fields.put(storageName, new ArrayOfObjectField(ref.valueType(), o, column, arraySourceParser));
                return;
            }
        }
        fields.put(storageName, new ValueField(ref.valueType(), column));
    }

    public boolean shouldLoadStoredFields() {
        return fields.isEmpty() == false;
    }

    public void reset() {
        this.doc = new HashMap<>();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return fields.containsKey(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        var field = fields.get(fieldInfo.name);
        var v = field.sanitize(value);
        this.doc.put(field, v);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.sanitize(value));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.sanitize(value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.sanitize(value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.sanitize(value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.sanitize(value));
    }

    public Map<String, Object> getDocMap() {
        Map<String, Object> docMap = new HashMap<>();
        this.doc.forEach((field, v) -> Maps.mergeInto(docMap, field.column().name(), field.column().path(), v));
        return docMap;
    }
}

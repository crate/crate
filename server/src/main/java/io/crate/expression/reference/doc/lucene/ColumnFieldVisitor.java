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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import io.crate.common.collections.Maps;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public class ColumnFieldVisitor extends StoredFieldVisitor {

    public ColumnFieldVisitor(Set<Reference> droppedColumns) {
        this.droppedColumns
            = droppedColumns.stream().map(Reference::column).collect(Collectors.toUnmodifiableSet());
    }

    private interface Field {
        Object sanitize(Object v);
        ColumnIdent column();
    }

    private record ValueField(DataType<?> dataType, ColumnIdent column) implements Field {
        @Override
        public Object sanitize(Object v) {
            return dataType.sanitizeValue(v);
        }
    }

    private record ArrayOfObjectField(DataType<?> dataType, Set<ColumnIdent> droppedColumns, ColumnIdent column) implements Field {
        @Override
        public Object sanitize(Object v) {
            var parsed = dataType.sanitizeValue(v);
            if (parsed instanceof List<?> l) {
                for (var entry : l) {
                    if (entry instanceof Map<?, ?> m) {
                        for (ColumnIdent col : droppedColumns) {
                            Maps.removeByPath((Map<String, ?>) m, col.path());
                        }
                    }
                }
            }
            return parsed;
        }
    }

    private final Map<String, Field> fields = new HashMap<>();
    private final Set<ColumnIdent> droppedColumns;

    private Map<String, Object> doc = new HashMap<>();

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
            if (ArrayType.unnest(ref.valueType()) instanceof ObjectType) {
                fields.put(storageName, new ArrayOfObjectField(ref.valueType(), findChildDroppedColumns(ref.column()), column));
                return;
            }
        }
        fields.put(storageName, new ValueField(ref.valueType(), column));
    }

    private Set<ColumnIdent> findChildDroppedColumns(ColumnIdent base) {
        Set<ColumnIdent> childDroppedColumns = new HashSet<>();
        for (var col : droppedColumns) {
            if (col.isChildOf(base)) {
                childDroppedColumns.add(col.shiftTo(base));
            }
        }
        return childDroppedColumns;
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
        Maps.mergeInto(this.doc, field.column().name(), field.column().path(), v);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column().name(), field.column().path(), field.sanitize(value));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column().name(), field.column().path(), field.sanitize(value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column().name(), field.column().path(), field.sanitize(value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column().name(), field.column().path(), field.sanitize(value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column().name(), field.column().path(), field.sanitize(value));
    }

    public Map<String, Object> getDocMap() {
        return doc;
    }
}

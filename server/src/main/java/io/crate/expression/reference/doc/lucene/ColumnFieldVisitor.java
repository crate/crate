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
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import io.crate.common.collections.Maps;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

public class ColumnFieldVisitor extends StoredFieldVisitor {

    private record Field(DataType<?> dataType, ColumnIdent column) {}

    private final Map<String, Field> fields = new HashMap<>();

    private Map<String, Object> doc = new HashMap<>();

    public void registerRef(Reference ref) {
        var column = ref.column();
        if (column.name().equals(DocSysColumns.Names.DOC)) {
            column = column.shiftRight();
        }
        var storageName = ref.storageIdent();
        if (ref.valueType() instanceof ArrayType<?>) {
            storageName = ArrayIndexer.ARRAY_VALUES_FIELD_PREFIX + storageName;
        }
        fields.put(storageName, new Field(ref.valueType(), column));
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
        Maps.mergeInto(this.doc, field.column.name(), field.column.path(), field.dataType.sanitizeValue(value));
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column.name(), field.column.path(), field.dataType.sanitizeValue(value));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column.name(), field.column.path(), field.dataType.sanitizeValue(value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column.name(), field.column.path(), field.dataType.sanitizeValue(value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column.name(), field.column.path(), field.dataType.sanitizeValue(value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        var field = fields.get(fieldInfo.name);
        Maps.mergeInto(this.doc, field.column.name(), field.column.path(), field.dataType.sanitizeValue(value));
    }

    public Map<String, Object> getDocMap() {
        return doc;
    }
}

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
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.Version;
import org.jetbrains.annotations.NotNull;

import io.crate.common.collections.Maps;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.ArrayType;
import io.crate.types.StorageSupport;

/**
 * Loads column values from stored fields and presents them as a java Map
 */
public class ColumnFieldVisitor extends StoredFieldVisitor {

    private final Map<String, Field> fields = new HashMap<>();
    private final Set<ColumnIdent> droppedColumns;
    private final SourceParser storedSourceParser;
    private final Version tableVersion;

    /**
     * Creates a new ColumnFieldVisitor for the given table
     */
    public ColumnFieldVisitor(DocTableInfo table) {
        this.droppedColumns
            = table.droppedColumns().stream().map(Reference::column).collect(Collectors.toUnmodifiableSet());
        this.storedSourceParser = new SourceParser(table.droppedColumns(), table.lookupNameBySourceKey(), true);
        this.tableVersion = table.versionCreated();
    }

    private record Field(StorageSupport<?> storageSupport, ColumnIdent column) implements Comparable<Field> {

        public Object decode(SourceParser sourceParser, Version tableVersion, byte[] v) {
            return storageSupport.decode(column, sourceParser, tableVersion, v);
        }

        public Object decode(long v) {
            return storageSupport.decode(v);
        }

        public Object decode(int v) {
            return storageSupport.decode(v);
        }

        @Override
        public int compareTo(@NotNull ColumnFieldVisitor.Field o) {
            return this.column.compareTo(o.column);
        }
    }

    // Maps.mergeInto() needs its inputs to be sorted, to ensure that a parent object o doesn't overwrite
    // an already written child o['child'], so we read stored fields into a sorted map and then
    // iterate them in column order when converting them into a docMap.
    private Map<Field, Object> doc = new TreeMap<>();

    /**
     * Ensure that the given column is loaded from stored fields
     */
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
        }
        fields.put(storageName, new Field(ref.valueType().storageSupportSafe(), column));
    }

    /**
     * @return {@code true} if columns requiring stored fields have been registered
     */
    public boolean shouldLoadStoredFields() {
        return fields.isEmpty() == false;
    }

    /**
     * Prepare to load a new row by clearing the doc state
     */
    public void reset() {
        this.doc = new TreeMap<>();
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return fields.containsKey(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        var field = fields.get(fieldInfo.name);
        var v = field.decode(storedSourceParser, tableVersion, value);
        this.doc.put(field, v);
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.decode(value));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, field.decode(value));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        var field = fields.get(fieldInfo.name);
        this.doc.put(field, value);
    }

    /**
     * @return the columns from the current row as a java Map
     */
    public Map<String, Object> getDocMap() {
        Map<String, Object> docMap = new HashMap<>();
        this.doc.forEach((field, v) -> Maps.mergeInto(docMap, field.column().name(), field.column().path(), v));
        return docMap;
    }
}

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.Version;

import io.crate.common.collections.Maps;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ScopedRef;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.SysColumns;
import io.crate.types.ArrayType;
import io.crate.types.StorageSupport;

/**
 * Loads column values from stored fields and presents them as a java Map
 */
public class ColumnFieldVisitor extends StoredFieldVisitor {

    private static final Comparator<Record> CMP_RECORD = new Comparator<Record>() {

        @Override
        public int compare(Record o1, Record o2) {
            return o1.column.compareTo(o2.column);
        }
    };
    private final Map<String, Field> fields = new HashMap<>();
    private final SourceParser storedSourceParser;
    private final Version shardVersion;

    /**
     * Creates a new ColumnFieldVisitor for the given table
     */
    public ColumnFieldVisitor(DocTableInfo table, Version shardVersionCreated) {
        this.storedSourceParser = new SourceParser(table.lookupNameBySourceKey(), true);
        this.shardVersion = shardVersionCreated;
    }

    private record Field(StorageSupport<?> storageSupport, ColumnIdent column) {

        public Object decode(SourceParser sourceParser, Version tableVersion, byte[] v) {
            return storageSupport.decode(column, sourceParser, tableVersion, v);
        }

        public Object decode(long v) {
            return storageSupport.decode(v);
        }

        public Object decode(int v) {
            return storageSupport.decode(v);
        }
    }

    private record Record(ColumnIdent column, Object value) {
    }

    // Maps.mergeInto() needs its inputs to be sorted, to ensure that a parent object o doesn't overwrite
    // an already written child o['child'], so we read stored fields into a list and then
    // sort it in column order when converting them into a docMap.
    private ArrayList<Record> records = new ArrayList<>();

    /**
     * Ensure that the given column is loaded from stored fields
     */
    public void registerRef(ScopedRef ref) {
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
        this.records = new ArrayList<>(records.size());
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        return fields.containsKey(fieldInfo.name) ? Status.YES : Status.NO;
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        var field = fields.get(fieldInfo.name);
        var v = field.decode(storedSourceParser, shardVersion, value);
        records.add(new Record(field.column, v));
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) throws IOException {
        var field = fields.get(fieldInfo.name);
        records.add(new Record(field.column, value));
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        var field = fields.get(fieldInfo.name);
        records.add(new Record(field.column, field.decode(value)));
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        var field = fields.get(fieldInfo.name);
        records.add(new Record(field.column, field.decode(value)));
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        var field = fields.get(fieldInfo.name);
        records.add(new Record(field.column, value));
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        var field = fields.get(fieldInfo.name);
        records.add(new Record(field.column, value));
    }

    /**
     * @return the columns from the current row as a java Map
     */
    public Map<String, Object> getDocMap() {
        HashMap<String, Object> docMap = HashMap.newHashMap(records.size());
        records.sort(CMP_RECORD);
        for (int i = 0; i < records.size(); i++) {
            Record record = records.get(i);
            Maps.mergeInto(docMap, record.column.name(), record.column.path(), record.value);
        }
        return docMap;
    }
}

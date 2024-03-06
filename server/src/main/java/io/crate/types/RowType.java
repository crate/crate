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

package io.crate.types;

import io.crate.Streamer;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.statistics.ColumnStatsSupport;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class RowType extends DataType<Row> implements Streamer<Row> {

    public static final int ID = 18;
    public static final String NAME = "record";
    public static final RowType EMPTY = new RowType(List.of());

    private final List<DataType<?>> fieldTypes;
    private final List<String> fieldNames;

    /**
     * @param fieldTypes the types of the fields within a Row
     * @param fieldNames the names of the fields. This can be empty in which case defaults will be created (colX ... colN)
     */
    public RowType(List<DataType<?>> fieldTypes, List<String> fieldNames) {
        assert fieldNames.isEmpty() || fieldNames.size() == fieldTypes.size()
            : "fieldNames must either be empty or have the same length as fieldTypes";
        this.fieldTypes = fieldTypes;
        if (fieldNames.isEmpty() && !fieldTypes.isEmpty()) {
            ArrayList<String> generatedFieldNames = new ArrayList<>(fieldTypes.size());
            for (int i = 0; i < fieldTypes.size(); i++) {
                generatedFieldNames.add("col" + (i + 1));
            }
            this.fieldNames = List.copyOf(generatedFieldNames);
        } else {
            this.fieldNames = List.copyOf(fieldNames);
        }
    }

    public RowType(List<DataType<?>> fieldTypes) {
        this(fieldTypes, List.of());
    }

    public RowType(StreamInput in) throws IOException {
        int numFields = in.readVInt();
        ArrayList<DataType<?>> fieldTypes = new ArrayList<>(numFields);
        ArrayList<String> fieldNames = new ArrayList<>(numFields);
        for (int i = 0; i < numFields; i++) {
            fieldTypes.add(DataTypes.fromStream(in));
            fieldNames.add(in.readString());
        }
        this.fieldTypes = List.copyOf(fieldTypes);
        this.fieldNames = List.copyOf(fieldNames);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(fieldTypes.size());
        for (int i = 0; i < fieldTypes.size(); i++) {
            DataTypes.toStream(fieldTypes.get(i), out);
            out.writeString(fieldNames.get(i));
        }
    }

    public int numElements() {
        return fieldTypes.size();
    }

    public DataType<?> getFieldType(int position) {
        return fieldTypes.get(position);
    }

    public String getFieldName(int position) {
        return fieldNames.get(position);
    }

    public List<String> fieldNames() {
        return fieldNames;
    }

    public List<DataType<?>> fieldTypes() {
        return fieldTypes;
    }

    @Override
    public Row readValueFrom(StreamInput in) throws IOException {
        Object[] values = new Object[fieldTypes.size()];
        for (int i = 0; i < fieldTypes.size(); i++) {
            values[i] = fieldTypes.get(i).streamer().readValueFrom(in);
        }
        return new RowN(values);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void writeValueTo(StreamOutput out, Row row) throws IOException {
        assert row.numColumns() == fieldTypes.size()
            : "Row that should be streamed must have the same number of columns as the rowType contains fieldTypes";
        for (int i = 0; i < fieldTypes.size(); i++) {
            Streamer streamer = fieldTypes.get(i).streamer();
            streamer.writeValueTo(out, row.get(i));
        }
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.TABLE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<Row> streamer() {
        return this;
    }

    @Override
    public Row implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return (Row) value;
    }

    @Override
    public Row sanitizeValue(Object value) {
        return (Row) value;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public int compare(Row val1, Row val2) {
        assert val1.numColumns() == val2.numColumns()
            : "Rows to compare must have the same number of columns and types. val1=" + val1 + ", val2=" + val2;
        for (int i = 0; i < fieldTypes.size(); i++) {
            DataType dataType = fieldTypes.get(i);
            int cmp = dataType.compare(val1.get(i), val2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    @Override
    public TypeSignature getTypeSignature() {
        ArrayList<TypeSignature> parameters = new ArrayList<>(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++) {
            var fieldName = fieldNames.get(i);
            var fieldType = fieldTypes.get(i);
            parameters.add(
                new ParameterTypeSignature(
                    fieldName,
                    fieldType.getTypeSignature()
                )
            );
        }
        return new TypeSignature(NAME, parameters);
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        return fieldTypes;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public long valueBytes(Row value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
        }
        long size = 0L;
        for (int i = 0; i < value.numColumns(); i++) {
            Object object = value.get(i);
            DataType dataType = fieldTypes.get(i);
            size += dataType.valueBytes(object);
        }
        return size;
    }
}

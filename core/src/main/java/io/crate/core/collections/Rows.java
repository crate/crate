/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.core.collections;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.*;

public class Rows implements Iterable<Row>, Streamable {

    private List<Row> entries;
    private DataType[] dataTypes;
    private Map<Integer, Integer> columnIndicesMap;
    private boolean streamed = false;

    Rows() {
    }

    public Rows(int initialCapacity, DataType[] dataTypes, List<Integer> columnIndicesToStream) {
        entries = new ArrayList<>(initialCapacity);
        this.dataTypes = dataTypes;
        Collections.sort(columnIndicesToStream);
        this.columnIndicesMap = new HashMap<>(columnIndicesToStream.size());
        for (int i = 0; i < columnIndicesToStream.size(); i++) {
            columnIndicesMap.put(columnIndicesToStream.get(i), i);
        }
    }

    public Rows(DataType[] dataTypes, List<Integer> columnIndicesToStream) {
        this(10, dataTypes, columnIndicesToStream);
    }

    public boolean addSafe(Row row) {
        // TODO: make entries a Bucket, so we do not need to copy here
        return entries.add(new Entry(row.materialize()));
    }

    public boolean add(Object[] row) {
        return entries.add(new Entry(row));
    }

    @Override
    public Iterator<Row> iterator() {
        return entries.iterator();
    }

    public static Rows fromStream(StreamInput in) throws IOException {
        Rows rows = new Rows();
        rows.readFrom(in);
        return rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        streamed = true;
        int columnSize = in.readVInt();
        dataTypes = new DataType[columnSize];
        columnIndicesMap = new HashMap<>(columnSize);
        for (int i = 0; i < columnSize; i++) {
            dataTypes[i] = DataTypes.fromStream(in);
        }
        for (int i = 0; i < columnSize; i++) {
            columnIndicesMap.put(in.readVInt(), in.readVInt());
        }
        int rowsSize = in.readVInt();
        entries = new ArrayList<>(rowsSize);
        for (int i = 0; i < rowsSize; i++) {
            Entry row = new Entry();
            row.readFrom(in);
            entries.add(row);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(dataTypes.length);
        for (DataType dataType : dataTypes) {
            DataTypes.toStream(dataType, out);
        }
        for (Map.Entry<Integer, Integer> entry : columnIndicesMap.entrySet()) {
            out.writeVInt(entry.getKey());
            out.writeVInt(entry.getValue());
        }
        out.writeVInt(entries.size());
        for (Row row : this) {
            ((Entry) row).writeTo(out);
        }
    }

    public class Entry implements Row, Streamable {

        private Object[] row;

        Entry() {
        }

        public Entry(Object[] row) {
            this.row = row;
        }

        @Override
        public int size() {
            return row.length;
        }

        @Override
        public Object get(int index) {
            if (streamed) {
                assert columnIndicesMap.get(index) <= row.length;
                return row[columnIndicesMap.get(index)];
            }
            return row[index];
        }

        @Override
        public Object[] materialize() {
            return row;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            row = new Object[dataTypes.length];
            for (int j = 0; j < dataTypes.length; j++) {
                row[j] = dataTypes[j].streamer().readValueFrom(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            for (int i = 0; i < row.length; i++) {
                if (columnIndicesMap.get(i) != null) {
                    dataTypes[columnIndicesMap.get(i)].streamer().writeValueTo(out, row[i]);
                }
            }
        }
    }
}

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

package io.crate.statistics.arrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructReaderImpl;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.io.stream.StreamOutput;


import io.crate.metadata.ColumnIdent;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.types.DataTypes;

public class Statistics implements AutoCloseable {

    private final VectorSchemaRoot root;

    public Statistics(BufferAllocator bufferAllocator,
                      long numDocs,
                      long sizeInBytes,
                      Map<ColumnIdent, ColumnStats<?>> statsByColumn) {
        this.root = VectorSchemaRoot.create(schema(), bufferAllocator);

        BigIntVector numDocsVector = (BigIntVector) root.getVector("numDocs");
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, numDocs);
        numDocsVector.setValueCount(1);

        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector("sizeInBytes");
        sizeInBytesVector.allocateNew(1);
        sizeInBytesVector.set(0, sizeInBytes);
        sizeInBytesVector.setValueCount(1);

        StructVector statsByColumnVector = (StructVector) root.getVector("statsByColumn");
        statsByColumnVector.allocateNew();

        NullableStructWriter structWriter = statsByColumnVector.getWriter();
        structWriter.allocate();

        for (Map.Entry<ColumnIdent, ColumnStats<?>> columnIdentColumnStatsEntry : statsByColumn.entrySet()) {
            ColumnIdent columnIdent = columnIdentColumnStatsEntry.getKey();
            ColumnStats<?> columnStats = columnIdentColumnStatsEntry.getValue();
            structWriter.start();
            structWriter.varChar("columnIdent").writeVarChar(columnIdent.fqn());
            structWriter.float8("nullFraction").writeFloat8(columnStats.nullFraction());
            structWriter.float8("averageSizeInBytes").writeFloat8(columnStats.averageSizeInBytes());
            structWriter.float8("approxDistinct").writeFloat8(columnStats.approxDistinct());
            structWriter.integer("type").writeInt(columnStats.type().id());
            structWriter.end();
        }

        structWriter.setValueCount(statsByColumn.size());
        statsByColumnVector.setValueCount(statsByColumn.size());
    }

    private Statistics(VectorSchemaRoot root) {
        this.root = root;
    }

    public Statistics(BufferAllocator bufferAllocator, InputStream in) throws IOException {
        try (ArrowStreamReader reader = new ArrowStreamReader(in, bufferAllocator)) {
            reader.loadNextBatch();
            try (VectorSchemaRoot source = reader.getVectorSchemaRoot()) {
                VectorUnloader unloader = new VectorUnloader(source);
                VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), bufferAllocator);
                VectorLoader loader = new VectorLoader(copy);
                try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
                    loader.load(recordBatch);
                    this.root = copy;
                }
            }
        }
    }

    public long numDocs() {
        BigIntVector numDocsVector = (BigIntVector) root.getVector("numDocs");
        return numDocsVector.get(0);
    }

    public long sizeInBytes() {
        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector("sizeInBytes");
        return sizeInBytesVector.get(0);
    }

    public Map<ColumnIdent, ColumnStats<?>> statsByColumn() {
        Map<ColumnIdent, ColumnStats<?>> result = new HashMap<>();
        StructVector statsByColumn = (StructVector) root.getVector("statsByColumn");
        NullableStructReaderImpl structReader = statsByColumn.getReader();
        for (int i = 0; i < statsByColumn.getValueCount(); i++) {
            structReader.setPosition(i);
            ColumnIdent columnIdent = ColumnIdent.of(structReader.reader("columnIdent").readText().toString());

            ColumnStats<?> columnStats = new ColumnStats<>(
                structReader.reader("nullFraction").readDouble(),
                structReader.reader("averageSizeInBytes").readDouble(),
                structReader.reader("approxDistinct").readDouble(),
                DataTypes.fromId(structReader.reader("type").readInteger()),
                MostCommonValues.empty(),
                List.of()
            );
            result.put(columnIdent, columnStats);
        }
        return result;
    }

    static Schema schema() {
        List<Field> fields = new ArrayList<>();
        fields.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        fields.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));

        List<Field> columnStatsFields = new ArrayList<>();
        columnStatsFields.add(Field.notNullable("columnIdent", new ArrowType.Utf8()));
        columnStatsFields.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStatsFields.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStatsFields.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStatsFields.add(Field.notNullable("type", new ArrowType.Int(32, true)));

        fields.add(new Field("statsByColumn", FieldType.notNullable(new ArrowType.Struct()), columnStatsFields));
        return new Schema(fields);
    }

    public long averageSizePerRowInBytes() {
        long numDocs = numDocs();
        if (numDocs == -1) {
            return -1;
        } else if (numDocs == 0) {
            return 0;
        } else {
            return sizeInBytes() / numDocs;
        }
    }

    public void write(File file) throws IOException {
        try (
            FileOutputStream out = new FileOutputStream(file);
            ArrowStreamWriter writer = new ArrowStreamWriter(this.root, null, out.getChannel());
        ) {
            writer.start();
            writer.writeBatch();
        }
    }

    public static Statistics read(BufferAllocator bufferAllocator, File file) throws IOException {
        try (
            FileInputStream fileInputStreamForStream = new FileInputStream(file);
            ArrowStreamReader reader = new ArrowStreamReader(fileInputStreamForStream, bufferAllocator)
        ) {
            reader.loadNextBatch();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            return new Statistics(root);
        }
    }

    public void write(StreamOutput out) throws IOException {
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
        }
    }

    public void close() {
        this.root.close();
    }
}

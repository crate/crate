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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.BaseReader;
import org.apache.arrow.vector.complex.reader.Float8Reader;
import org.apache.arrow.vector.complex.reader.IntReader;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;


import io.crate.metadata.ColumnIdent;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.types.DataTypes;

public class Statistics implements AutoCloseable {

//    public static Statistics EMPTY = new Statistics();

    private final VectorSchemaRoot root;

    private Statistics() {
        this(-1, -1, Map.of());
    }

    public Statistics(long numDocs,
                      long sizeInBytes,
                      Map<ColumnIdent, ColumnStats<?>> statsByColumn) {
        this.root = VectorSchemaRoot.create(schema(), Allocator.INSTANCE);
        BigIntVector numDocsVector = (BigIntVector) root.getVector("numDocs");
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, numDocs);
        numDocsVector.setValueCount(1);

        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector("sizeInBytes");
        sizeInBytesVector.allocateNew(1);
        sizeInBytesVector.set(0, sizeInBytes);
        sizeInBytesVector.setValueCount(1);

        ListVector statsByColumnVector = (ListVector) root.getVector("statsByColumn");
        UnionListWriter listWriter = statsByColumnVector.getWriter();
        listWriter.startList();
        listWriter.setPosition(0);
        BaseWriter.StructWriter structWriter = listWriter.struct();
        int index = 0;
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
            index++;
        }
        listWriter.endList();
        listWriter.setValueCount(index);
        root.setRowCount(1);
    }

    public Statistics(InputStream in) throws IOException {
        try (ArrowStreamReader reader = new ArrowStreamReader(in, Allocator.INSTANCE)) {
            reader.loadNextBatch();
            try (VectorSchemaRoot source = reader.getVectorSchemaRoot()) {
                VectorUnloader unloader = new VectorUnloader(source);
                VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), Allocator.INSTANCE);
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
        ListVector statsByColumn = (ListVector) root.getVector("statsByColumn");
        UnionListReader listReader = statsByColumn.getReader();
        for (int i = 0; i < statsByColumn.getValueCount(); i++) {
            listReader.setPosition(i);
            while (listReader.next()) {
                BaseReader.StructReader structReader = listReader.reader();
                VarCharReader columnIdenReader = structReader.reader("columnIdent");
                ColumnIdent columnIdent = ColumnIdent.of(columnIdenReader.readText().toString());
                Float8Reader nullFraction = structReader.reader("nullFraction");
                Float8Reader averageSizeInBytes = structReader.reader("averageSizeInBytes");
                Float8Reader approxDistinct = structReader.reader("approxDistinct");
                IntReader type = structReader.reader("type");
                ColumnStats<?> columnStats = new ColumnStats<>(
                    nullFraction.readDouble(),
                    averageSizeInBytes.readDouble(),
                    approxDistinct.readDouble(),
                    DataTypes.fromId(type.readInteger()),
                    MostCommonValues.empty(),
                    List.of()
                );
                result.put(columnIdent, columnStats);
            }
        }
        return result;
    }

    @Nullable
    public ColumnStats<?> getColumnStats(ColumnIdent column) {
        return statsByColumn().get(column);
    }

    private static Schema schema() {
        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));

        List<Field> columnStats = new ArrayList<>();
        columnStats.add(Field.notNullable("columnIdent", new ArrowType.Utf8()));
        columnStats.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("type", new ArrowType.Int(32, true)));

        Field columnStatsStruct = new Field("statsByColumn", FieldType.nullable(new ArrowType.Struct()), columnStats);
        FieldType listType = new FieldType(true, new ArrowType.List(), null);
        stats.add(new Field("statsByColumn", listType, List.of(columnStatsStruct)));
        return new Schema(stats);
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

    public void close() {
        this.root.close();
    }

    public void write(StreamOutput out) throws IOException {
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
            writer.start();
            writer.writeBatch();
        }
    }
}

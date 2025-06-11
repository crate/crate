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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;


import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.types.DataTypes;

public class Statistics {

    private static final Field RELATION_NAME = Field.notNullable("relationName", new ArrowType.Utf8());
    private static final Field NUM_DOCS = Field.notNullable("numDocs", new ArrowType.Int(64, true));
    private static final Field SIZE_IN_BYTES = Field.notNullable("sizeInBytes", new ArrowType.Int(64, true));

    private final VectorSchemaRoot root;

    public Statistics(RootAllocator allocator, InputStream in) throws IOException {
        try (
            ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
        ) {
            reader.loadNextBatch();
            final VectorSchemaRoot source = reader.getVectorSchemaRoot();
            final VectorUnloader unloader = new VectorUnloader(source);
            final VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
            final VectorLoader loader = new VectorLoader(copy);
            loader.load(unloader.getRecordBatch());
            this.root = copy;
//            System.out.println("root.contentToTSVString() = " + root.contentToTSVString());
        }
    }

    public Statistics(RootAllocator rootAllocator,
                      long numDocs,
                      long sizeInBytes,
                      Map<ColumnIdent, ColumnStats<?>> statsByColumn) {
        this.root = VectorSchemaRoot.create(schema(), rootAllocator);
//        VarCharVector relationNameVector = (VarCharVector) root.getVector(RELATION_NAME);
//        relationNameVector.allocateNew(1);
//        relationNameVector.set(0, relationName.fqn().getBytes());
        BigIntVector numDocsVector = (BigIntVector) root.getVector("numDocs");
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, numDocs);
//        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector(SIZE_IN_BYTES);
//        sizeInBytesVector.allocateNew(1);
//        sizeInBytesVector.set(0, sizeInBytes);
//        ListVector statsByColumnVector = (ListVector) root.getVector("statsByColumn");
//        UnionListWriter listWriter = statsByColumnVector.getWriter();
//        listWriter.startList();
//        int index = 0;
//        for (Map.Entry<ColumnIdent, ColumnStats<?>> columnIdentColumnStatsEntry : statsByColumn.entrySet()) {
//            ColumnIdent columnIdent = columnIdentColumnStatsEntry.getKey();
//            ColumnStats<?> columnStats = columnIdentColumnStatsEntry.getValue();
//            listWriter.setPosition(index);
//            BaseWriter.StructWriter struct = listWriter.struct();
//            struct.start();
//            struct.varChar("columnIdent").writeVarChar(columnIdent.fqn());
//            struct.float8("nullFraction").writeFloat8(columnStats.nullFraction());
//            struct.float8("averageSizeInBytes").writeFloat8(columnStats.averageSizeInBytes());
//            struct.float8("approxDistinct").writeFloat8(columnStats.approxDistinct());
//            struct.integer("type").writeInt(columnStats.type().id());
//            struct.end();
//            index++;
//        }
//        listWriter.endList();
//        listWriter.setValueCount(index);
        root.setRowCount(1);
    }

    public long numDocs() {
        BigIntVector numDocsVector = (BigIntVector) root.getVector("numDocs");
        return numDocsVector.get(0);
    }

    public long sizeInBytes() {
        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector(SIZE_IN_BYTES);
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
                ColumnIdent columnIdent  = ColumnIdent.of(columnIdenReader.readText().toString());
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
//        stats.add(Field.notNullable("relationName", new ArrowType.Utf8()));
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
        stats.add(new Field("statsByColumn",listType, List.of(columnStatsStruct)));
        return new Schema(stats);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Statistics that = (Statistics) o;
        return Objects.equals(root, that.root);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(root);
    }

    public void write(StreamOutput out) throws IOException {
        try (
            ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out));
        ) {
            writer.start();
            writer.writeBatch();
        }
    }
}

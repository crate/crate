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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.BaseReader;
import org.apache.arrow.vector.complex.reader.Float8Reader;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jetbrains.annotations.Nullable;


import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnStats;

public class Statistics {

    private static final Field RELATION_NAME = Field.notNullable("relationName", new ArrowType.Utf8());
    private static final Field NUM_DOCS = Field.notNullable("numDocs", new ArrowType.Int(64, true));
    private static final Field SIZE_IN_BYTES = Field.notNullable("sizeInBytes", new ArrowType.Int(64, true));

    private static final Field COLUMN_IDENT = Field.notNullable("columnIdent", new ArrowType.Utf8()));
    private static final Field NULL_FRACTION = Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    private static final Field AVERAGE_SIZE_IN_BYTES = Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
    private static final Field APPROX_DISTINCT = Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

    private final VectorSchemaRoot root;

    public Statistics(BufferAllocator bufferAllocator,
                      RelationName relationName,
                      long numDocs,
                      long sizeInBytes,
                      Map<ColumnIdent, ColumnStats<?>> statsByColumn) {
        root = VectorSchemaRoot.create(schema(), bufferAllocator);
        VarCharVector relationNameVector = (VarCharVector) root.getVector(RELATION_NAME);
        relationNameVector.allocateNew(1);
        relationNameVector.set(0, relationName.fqn().getBytes());
        BigIntVector numDocsVector = (BigIntVector) root.getVector(NUM_DOCS);
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, numDocs);
        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector(SIZE_IN_BYTES);
        sizeInBytesVector.allocateNew(1);
        sizeInBytesVector.set(0, sizeInBytes);
        int index = 0;
        for (Map.Entry<ColumnIdent, ColumnStats<?>> columnIdentColumnStatsEntry : statsByColumn.entrySet()) {
            ColumnIdent columnIdent = columnIdentColumnStatsEntry.getKey();
            ColumnStats<?> columnStats = columnIdentColumnStatsEntry.getValue();
            ListVector statsByColumnVector = (ListVector) root.getVector("statsByColumn");
            UnionListWriter listWriter = statsByColumnVector.getWriter();
            listWriter.startList();
            listWriter.setPosition(index);
            BaseWriter.StructWriter struct = listWriter.struct();
            struct.start();
            struct.varChar("columnIdent").writeVarChar(columnIdent.fqn());
            struct.float8("nullFraction").writeFloat8(columnStats.nullFraction());
            struct.float8("averageSizeInBytes").writeFloat8(columnStats.averageSizeInBytes());
            struct.float8("approxDistinct").writeFloat8(columnStats.approxDistinct());
            struct.end();
            listWriter.endList();
            listWriter.setValueCount(index);
            index++;
        }
        root.setRowCount(1);
    }

    public long numDocs() {
        BigIntVector numDocsVector = (BigIntVector) root.getVector(NUM_DOCS);
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
                Float8Reader nullFraction = (Float8Reader) structReader.reader("nullFraction");
                Float8Reader averageSizeInBytes = (Float8Reader) structReader.reader("averageSizeInBytes");
                Float8Reader approxDistinct = (Float8Reader) structReader.reader("approxDistinct");
                ColumnStats<?> columnStats = new ColumnStats<?>(
                    nullFraction.readDouble(),
                    averageSizeInBytes.readDouble(),
                    approxDistinct.readDouble(),
            }
        }
    }

    @Nullable
    public ColumnStats<?> getColumnStats(ColumnIdent column) {
        return null;
    }

    private static Schema schema() {
        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("relationName", new ArrowType.Utf8()));
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));

        List<Field> columnStats = new ArrayList<>();
        columnStats.add(Field.notNullable("columnIdent", new ArrowType.Utf8()));
        columnStats.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        Field columnStatsStruct = new Field("statsByColumn", FieldType.nullable(new ArrowType.Struct()), columnStats);
        FieldType listType = new FieldType(true, new ArrowType.List(), null);
        stats.add(new Field("statsByColumn",listType, List.of(columnStatsStruct)));
        return new Schema(stats);
    }
}

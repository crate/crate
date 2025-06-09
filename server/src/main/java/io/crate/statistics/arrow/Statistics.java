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
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.jetbrains.annotations.Nullable;


import io.crate.metadata.ColumnIdent;
import io.crate.statistics.ColumnStats;

public class Statistics {

    private final VectorSchemaRoot vector;
    private final Map<ColumnIdent, ColumnStats<?>> statsByColumn;

    public Statistics(BufferAllocator bufferAllocator, long numDocs, long sizeInBytes, Map<ColumnIdent, ColumnStats<?>> statsByColumn) {
        vector = VectorSchemaRoot.create(schema(), bufferAllocator);
        BigIntVector numDocsVector = (BigIntVector) vector.getVector("numDocs");
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, numDocs);
        BigIntVector sizeInBytesVector = (BigIntVector) vector.getVector("sizeInBytes");
        sizeInBytesVector.allocateNew(1);
        sizeInBytesVector.set(0, sizeInBytes);
        vector.setRowCount(1);
        this.statsByColumn = statsByColumn;
    }

    public long numDocs() {
        BigIntVector numDocsVector = (BigIntVector) vector.getVector("numDocs");
        return numDocsVector.get(0);
    }

    public long sizeInBytes() {
        BigIntVector sizeInBytesVector = (BigIntVector) vector.getVector("sizeInBytes");
        return sizeInBytesVector.get(0);
    }

    public Map<ColumnIdent, ColumnStats<?>> statsByColumn() {
        return statsByColumn;
    }

    @Nullable
    public ColumnStats<?> getColumnStats(ColumnIdent column) {
        return statsByColumn.get(column);
    }

    private static Schema schema() {

        List<Field> columnStats = new ArrayList<>();
        columnStats.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("type", new ArrowType.Int(16, true)));

        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));
        stats.add(new Field("statsByColumn", FieldType.nullable(new ArrowType.Map(false)), columnStats));
        return new Schema(stats);
    }

    public static void main(String[] args) {

        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("relationName", new ArrowType.Utf8()));
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));

        List<Field> columnStats = new ArrayList<>();
        columnStats.add(Field.notNullable("columnIdent", new ArrowType.Utf8()));
        columnStats.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));

//        stats.add(new Field("statsByColumn", FieldType.nullable(new ArrowType.Map(false)), columnStats));
        Schema schema =  new Schema(stats);
        System.out.println("schema = " + schema);

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator());

        VarCharVector relationNameVector = (VarCharVector) root.getVector("relationName");

        relationNameVector.allocateNew(1);
        relationNameVector.set(0,"doc.test".getBytes());
        relationNameVector.setValueCount(1);

        BigIntVector numDocsVector = (BigIntVector) root.getVector("numDocs");
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, 100);
        numDocsVector.setValueCount(1);

        BigIntVector sizeInBytesVector = (BigIntVector) root.getVector("sizeInBytes");
        sizeInBytesVector.allocateNew(1);
        sizeInBytesVector.set(0, 1000);

        sizeInBytesVector.setValueCount(1);

        VarCharVector relationName = (VarCharVector) root.getVector("relationName");
        System.out.println("relationName = " + new String(relationName.get(0)));
        BigIntVector numDocs = (BigIntVector) root.getVector("numDocs");
        System.out.println("numDocs = " + numDocs.get(0));
        BigIntVector sizeInBytes = (BigIntVector) root.getVector("numDocs");
        System.out.println("sizeInBytes = " + sizeInBytes.get(0));




//        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null);
//        FieldType mapType = new FieldType(true, new ArrowType.Map(false), null);
//
//        Field keyField = new Field("key", stringType, null);
//        Field value1Field = new Field("value1", stringType, null);
//        Field value2Field = new Field("value2", stringType, null);
//
//        List<Field> childFields = new ArrayList<>();
//        childFields.add(keyField);
//        childFields.add(value1Field);
//        childFields.add(value2Field);
//        Field points = new Field("points", mapType, childFields);
//        System.out.print(points);
//
//        // map
//
//        MapVector vector = new MapVector("map", new RootAllocator(),
//            FieldType.nullable(new ArrowType.Map(false)), null);
//        BaseWriter.MapWriter writer = vector.getWriter();
//
//        writer.startMap();
//        writer.startEntry();
//        writer.key().integer().writeInt(1);
//        writer.value().varChar().writeVarChar("foo");
//        writer.endEntry();
//        writer.startEntry();
//        writer.key().integer().writeInt(2);
//        writer.value().varChar().writeVarChar("bar");
//        writer.endEntry();
//        writer.endMap();
//
//        vector.setValueCount(1);
//        System.out.println("vector = " + vector);
//        for (int i = 0; i < vector.getValueCount(); i++) {
//            if (!vector.isNull(i)) {
//                JsonStringHashMap elements = (JsonStringHashMap) vector.getObject(i);
//                System.out.println(elements);
//            }
//        }
    }
}

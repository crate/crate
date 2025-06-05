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

package io.crate.statistics.arrow;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class Stats {

    public static void main(String[] args) {
        Schema schema = simpleSchema();
        try (
            BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)
        ) {
            BigIntVector numDocs = (BigIntVector) root.getVector("numDocs");
            numDocs.allocateNew(1);
            numDocs.set(0, 1L);
            BigIntVector sizeInBytes = (BigIntVector) root.getVector("sizeInBytes");
            sizeInBytes.allocateNew(1);
            sizeInBytes.set(0, 10L);
            root.setRowCount(1);
            System.out.println("VectorSchemaRoot created: \n" + root.contentToTSVString());
        }
    }

    public static Schema schema() {
        List<Field> columnStats = new ArrayList<>();
        columnStats.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("type", new ArrowType.Int(16, true)));
        // add remaining fields

        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));
        stats.add(new Field("statsByColumn", FieldType.nullable(new ArrowType.Utf8()), columnStats));

        List<Field> tableStats = new ArrayList<>();
        stats.add(new Field("statsByColumn", FieldType.nullable(new ArrowType.Utf8()), columnStats));

        return new Schema(stats);
    }

    public static Schema simpleSchema() {
        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));
        return new Schema(stats);
    }
}

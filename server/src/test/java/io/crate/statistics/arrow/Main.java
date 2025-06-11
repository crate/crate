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

import static java.util.Arrays.asList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class Main {
    public static void main(String[] args) throws IOException {
        try (BufferAllocator rootAllocator = new RootAllocator()) {
            Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
            Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
            Schema schemaPerson = new Schema(asList(name, age));
            try (
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schemaPerson, rootAllocator)
            ) {
                VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
                nameVector.allocateNew(3);
                nameVector.set(0, "David".getBytes());
                nameVector.set(1, "Gladis".getBytes());
                nameVector.set(2, "Juan".getBytes());
                IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
                ageVector.allocateNew(3);
                ageVector.set(0, 10);
                ageVector.set(1, 20);
                ageVector.set(2, 30);
                vectorSchemaRoot.setRowCount(3);
                try (
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, null, Channels.newChannel(out))
                ) {
                    writer.start();
                    writer.writeBatch();
                    System.out.println("Number of rows written: " + vectorSchemaRoot.getRowCount());

                    try(
                        BufferAllocator allocator = new RootAllocator();
                        ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(
                            out.toByteArray()), allocator)
                    ) {
                        while(reader.loadNextBatch()){
                            System.out.print(reader.getVectorSchemaRoot().contentToTSVString());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }



    }
}

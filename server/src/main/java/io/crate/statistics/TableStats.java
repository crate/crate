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

package io.crate.statistics;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.crate.metadata.RelationName;
import io.crate.metadata.table.TableInfo;

public class TableStats implements AutoCloseable {

    private final BufferAllocator BUFFER_ALLOCATOR = new RootAllocator();
    private volatile Map<RelationName, VectorSchemaRoot> tableStats = new HashMap<>();

    public void updateTableStats(Map<RelationName, Stats> input) {
        for (Map.Entry<RelationName, Stats> entry : input.entrySet()) {
            RelationName relationName = entry.getKey();
            Stats stats = entry.getValue();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema(), this.BUFFER_ALLOCATOR);
            BigIntVector numDocs = (BigIntVector) root.getVector("numDocs");
            numDocs.allocateNew(1);
            numDocs.set(0, stats.numDocs());
            BigIntVector sizeInBytes = (BigIntVector) root.getVector("sizeInBytes");
            sizeInBytes.allocateNew(1);
            sizeInBytes.set(0, stats.sizeInBytes());
            root.setRowCount(1);
            tableStats.put(relationName, root);
        }
    }

    /**
     * Returns the number of docs a table has.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long numDocs(RelationName relationName) {
        VectorSchemaRoot result = tableStats.get(relationName);
        if (result != null) {
            BigIntVector numDocs = (BigIntVector) result.getVector("numDocs");
            return numDocs.get(0);
        }
        return Stats.EMPTY.numDocs();
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes.
     * <p>
     * <p>
     * The returned number isn't an accurate real-time value but a cached value that is periodically updated
     * </p>
     * Returns -1 if the table isn't in the cache
     */
    public long estimatedSizePerRow(RelationName relationName) {
        VectorSchemaRoot result = tableStats.get(relationName);
        if (result != null) {
            BigIntVector numDocs = (BigIntVector) result.getVector("sizeInBytes");
            return numDocs.get(0);
        }
        return Stats.EMPTY.numDocs();
    }

    /**
     * Returns an estimation (avg) size of each row of the table in bytes or if no stats are available
     * for the given table an estimate (avg) based on the column types of the table.
     */
    public long estimatedSizePerRow(TableInfo tableInfo) {
        return -1;
    }

    public Iterable<ColumnStatsEntry> statsEntries() {
        return null;
    }

    public io.crate.statistics.Stats getStats(RelationName relationName) {
        return null;
    }

    private static Schema schema() {
        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));
        return new Schema(stats);
    }


    @Override
    public void close() throws Exception {
        BUFFER_ALLOCATOR.close();
    }
}

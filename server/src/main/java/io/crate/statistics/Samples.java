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

package io.crate.statistics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;

class Samples implements Writeable {

    static final Samples EMPTY = new Samples(List.of(), 0L, 0L);

    private final List<ColumnSketch<?>> columnSketches;
    private final long numTotalDocs;
    private final long numTotalSizeInBytes;

    Samples(List<ColumnSketch<?>> columnSketches, long numTotalDocs, long numTotalSizeInBytes) {
        this.columnSketches = columnSketches;
        this.numTotalDocs = numTotalDocs;
        this.numTotalSizeInBytes = numTotalSizeInBytes;
    }

    public Samples(List<Reference> references, StreamInput in) throws IOException {
        if (in.getVersion().before(Version.V_5_7_0)) {
            throw new UnsupportedOperationException("Cannot run ANALYZE in a mixed version cluster");
        }
        this.numTotalDocs = in.readLong();
        this.numTotalSizeInBytes = in.readLong();
        int numRecords = in.readVInt();
        if (numRecords != references.size()) {
            throw new IllegalStateException(
                "Expected to receive stats for " + numRecords + " columns but received " + numRecords);
        }
        this.columnSketches = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Reference ref = references.get(i);
            this.columnSketches.add(new ColumnSketch<>(ref.valueType(), in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_5_7_0)) {
            throw new UnsupportedOperationException("Cannot run ANALYZE in a mixed version cluster");
        }
        out.writeLong(numTotalDocs);
        out.writeLong(numTotalSizeInBytes);
        out.writeVInt(columnSketches.size());
        for (ColumnSketch<?> stats : columnSketches) {
            stats.writeTo(out);
        }
    }

    public static Samples merge(int maxSampleSize, Samples s1, Samples s2, Random random) {
        if (s1 == Samples.EMPTY) {
            return s2;
        }
        if (s2 == Samples.EMPTY) {
            return s1;
        }
        if (s1.columnSketches.size() != s2.columnSketches.size()) {
            throw new IllegalArgumentException("Column mismatch");
        }
        List<ColumnSketch<?>> mergedColumns = new ArrayList<>();
        for (int i = 0; i < s1.columnSketches.size(); i++) {
            var merged = s1.columnSketches.get(i).merge(s2.columnSketches.get(i));
            mergedColumns.add(merged);
        }
        return new Samples(
            mergedColumns,
            s1.numTotalDocs + s2.numTotalDocs,
            s1.numTotalSizeInBytes + s2.numTotalSizeInBytes
        );
    }

    public Stats createTableStats(List<Reference> primitiveColumns) {
        Map<ColumnIdent, ColumnStats<?>> statsByColumn = new HashMap<>();
        for (int i = 0; i < primitiveColumns.size(); i++) {
            Reference primitiveColumn = primitiveColumns.get(i);
            statsByColumn.put(primitiveColumn.column(), columnSketches.get(i).toColumnStats());
        }
        return new Stats(numTotalDocs, numTotalSizeInBytes, statsByColumn);
    }
}

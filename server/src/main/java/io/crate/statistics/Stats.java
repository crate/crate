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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import io.crate.common.collections.Maps;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.types.FixedWidthType;

@VisibleForTesting
public class Stats implements Writeable {

    public static final Stats EMPTY = new Stats();

    @VisibleForTesting
    final long numDocs;
    @VisibleForTesting
    final long sizeInBytes;

    private final Map<ColumnIdent, ColumnStats<?>> statsByColumn;

    private Stats() {
        numDocs = -1;
        sizeInBytes = -1;
        statsByColumn = Map.of();
    }

    public Stats(long numDocs, long sizeInBytes, Map<ColumnIdent, ColumnStats<?>> statsByColumn) {
        this.numDocs = numDocs;
        this.sizeInBytes = sizeInBytes;
        this.statsByColumn = statsByColumn;
    }

    public Stats(StreamInput in) throws IOException {
        this.numDocs = in.readLong();
        this.sizeInBytes = in.readLong();
        int numColumnStats = in.readVInt();
        this.statsByColumn = HashMap.newHashMap(numColumnStats);
        for (int i = 0; i < numColumnStats; i++) {
            statsByColumn.put(ColumnIdent.of(in), new ColumnStats<>(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(numDocs);
        out.writeLong(sizeInBytes);
        out.writeVInt(statsByColumn.size());
        for (var entry : statsByColumn.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
    }

    public boolean isEmpty() {
        return numDocs == -1 && sizeInBytes == -1 && statsByColumn.isEmpty();
    }

    public Stats withNumDocs(long numDocs) {
        long sizePerRow = averageSizePerRowInBytes();
        if (sizePerRow < 1) {
            return new Stats(numDocs, -1, statsByColumn);
        } else {
            return new Stats(numDocs, sizePerRow * numDocs, statsByColumn);
        }
    }

    public Stats add(Stats other) {
        return new Stats(
            numDocs == -1 || other.numDocs == -1
                ? -1
                : numDocs + other.numDocs,
            sizeInBytes == -1 || other.sizeInBytes == -1
                ? -1
                : sizeInBytes + other.sizeInBytes,
            Maps.concat(statsByColumn, other.statsByColumn)
        );
    }

    public long numDocs() {
        return numDocs;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long averageSizePerRowInBytes() {
        if (numDocs == -1) {
            return -1;
        } else if (numDocs == 0) {
            return 0;
        } else {
            return sizeInBytes / numDocs;
        }
    }

    public Map<ColumnIdent, ColumnStats<?>> statsByColumn() {
        return statsByColumn;
    }

    @Nullable
    public ColumnStats<?> getColumnStats(ColumnIdent column) {
        return statsByColumn.get(column);
    }

    public long estimateSizeForColumns(Iterable<? extends Symbol> toCollect) {
        long sum = 0L;
        for (Symbol symbol : toCollect) {
            ColumnStats<?> columnStats = null;
            while (symbol instanceof AliasSymbol alias) {
                symbol = alias.symbol();
            }
            if (symbol instanceof Reference ref) {
                columnStats = statsByColumn.get(ref.column());
            } else if (symbol instanceof ScopedSymbol scopedSymbol) {
                columnStats = statsByColumn.get(scopedSymbol.column());
            }
            if (columnStats == null) {
                if (symbol.valueType() instanceof FixedWidthType fixedWidthType) {
                    sum += fixedWidthType.fixedSize();
                } else {
                    sum += RamUsageEstimator.UNKNOWN_DEFAULT_RAM_BYTES_USED;
                }
            } else {
                sum += (long) columnStats.averageSizeInBytes();
            }
        }
        return sum;
    }

    public static Stats fromVectorSchemaRoot(VectorSchemaRoot vector) {
        BigIntVector numDocsVector = (BigIntVector) vector.getVector("numDocs");
        long numDocs = numDocsVector.get(0);
        BigIntVector sizeInBytesVector = (BigIntVector) vector.getVector("sizeInBytes");
        long sizeInBytes = sizeInBytesVector.get(0);
        return new Stats(numDocs, sizeInBytes, Map.of());

    }

    public VectorSchemaRoot toVectorSchemaRoot(BufferAllocator bufferAllocator) {
        VectorSchemaRoot vector = VectorSchemaRoot.create(arrowSchema(), bufferAllocator);
        BigIntVector numDocsVector = (BigIntVector) vector.getVector("numDocs");
        numDocsVector.allocateNew(1);
        numDocsVector.set(0, numDocs);
        BigIntVector sizeInBytesVector = (BigIntVector) vector.getVector("sizeInBytes");
        sizeInBytesVector.allocateNew(1);
        sizeInBytesVector.set(0, sizeInBytes);
        vector.setRowCount(1);
        return vector;
    }

    private static Schema arrowSchema() {
        List<Field> columnStats = new ArrayList<>();
        columnStats.add(Field.notNullable("nullFraction", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("averageSizeInBytes", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("approxDistinct", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        columnStats.add(Field.notNullable("type", new ArrowType.Int(16, true)));
        List<Field> stats = new ArrayList<>();
        stats.add(Field.notNullable("numDocs", new ArrowType.Int(64, true)));
        stats.add(Field.notNullable("sizeInBytes", new ArrowType.Int(64, true)));
//        stats.add(new Field("statsByColumn", FieldType.nullable(new ArrowType.Map(false)), columnStats));
        return new Schema(stats);
    }
}

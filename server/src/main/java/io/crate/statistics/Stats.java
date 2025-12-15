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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jspecify.annotations.Nullable;

import io.crate.common.collections.Maps;
import io.crate.expression.symbol.AliasSymbol;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.types.FixedWidthType;

public record Stats(long numDocs,
                    long sizeInBytes,
                    Map<ColumnIdent, ColumnStats<?>> statsByColumn) implements Writeable {

    public static final Stats EMPTY = new Stats(-1, -1, Map.of());

    public static Stats readFrom(StreamInput in) throws IOException {
        long numDocs = in.readLong();
        long sizeInBytes = in.readLong();
        int numColumnStats = in.readVInt();
        Map<ColumnIdent, ColumnStats<?>> statsByColumn = HashMap.newHashMap(numColumnStats);
        for (int i = 0; i < numColumnStats; i++) {
            statsByColumn.put(ColumnIdent.of(in), new ColumnStats<>(in));
        }
        return new Stats(numDocs, sizeInBytes, statsByColumn);
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
}

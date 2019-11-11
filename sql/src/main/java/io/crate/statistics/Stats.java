/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.statistics;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.ColumnIdent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@VisibleForTesting
public class Stats implements Writeable {

    @VisibleForTesting
    final long numDocs;
    @VisibleForTesting
    final long sizeInBytes;

    private final Map<ColumnIdent, ColumnStats> statsByColumn;

    Stats() {
        numDocs = -1;
        sizeInBytes = -1;
        statsByColumn = Map.of();
    }

    public Stats(long numDocs, long sizeInBytes, Map<ColumnIdent, ColumnStats> statsByColumn) {
        this.numDocs = numDocs;
        this.sizeInBytes = sizeInBytes;
        this.statsByColumn = statsByColumn;
    }

    public Stats(StreamInput in) throws IOException {
        this.numDocs = in.readLong();
        this.sizeInBytes = in.readLong();
        int numColumnStats = in.readVInt();
        this.statsByColumn = new HashMap<>();
        for (int i = 0; i < numColumnStats; i++) {
            statsByColumn.put(new ColumnIdent(in), new ColumnStats(in));
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
}

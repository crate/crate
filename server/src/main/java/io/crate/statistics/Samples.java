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
import java.util.List;
import java.util.Random;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.Streamer;
import io.crate.common.collections.Lists;
import io.crate.data.Row;
import io.crate.data.RowN;

class Samples implements Writeable {

    static final Samples EMPTY = new Samples(List.of(), List.of(), 0L, 0L);

    @SuppressWarnings("rawtypes")
    private final List<Streamer> recordStreamer;
    final List<Row> records;
    final long numTotalDocs;
    final long numTotalSizeInBytes;

    @SuppressWarnings("rawtypes")
    Samples(List<Row> records, List<Streamer> recordStreamer, long numTotalDocs, long numTotalSizeInBytes) {
        this.records = records;
        this.recordStreamer = recordStreamer;
        this.numTotalDocs = numTotalDocs;
        this.numTotalSizeInBytes = numTotalSizeInBytes;
    }

    @SuppressWarnings("rawtypes")
    public Samples(List<Streamer> recordStreamer, StreamInput in) throws IOException {
        this.recordStreamer = recordStreamer;
        this.numTotalDocs = in.readLong();
        this.numTotalSizeInBytes = in.readLong();
        int numRecords = in.readVInt();
        this.records = new ArrayList<>(numRecords);
        for (int i = 0; i < numRecords; i++) {
            Object[] cells = new Object[recordStreamer.size()];
            for (int c = 0; c < cells.length; c++) {
                cells[c] = recordStreamer.get(c).readValueFrom(in);
            }
            this.records.add(new RowN(cells));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(numTotalDocs);
        out.writeLong(numTotalSizeInBytes);
        out.writeVInt(records.size());
        for (Row row : records) {
            assert row.numColumns() == recordStreamer.size()
                : "Number of columns in the row must match the number of streamers available";
            for (int i = 0; i < row.numColumns(); i++) {
                //noinspection unchecked
                recordStreamer.get(i).writeValueTo(out, row.get(i));
            }
        }
    }

    public static Samples merge(int maxSampleSize, Samples s1, Samples s2, Random random) {
        List<Row> newSamples = createNewSamples(maxSampleSize, s1, s2, random);
        return new Samples(
            newSamples,
            s1.recordStreamer.isEmpty() ? s2.recordStreamer : s1.recordStreamer,
            s1.numTotalDocs + s2.numTotalDocs,
            s1.numTotalSizeInBytes + s2.numTotalSizeInBytes
        );
    }

    private static List<Row> createNewSamples(int maxSampleSize, Samples s1, Samples s2, Random random) {
        if (s1.records.isEmpty()) {
            return s2.records;
        } else if (s2.records.isEmpty()) {
            return s1.records;
        }
        if (s1.records.size() + s2.records.size() <= maxSampleSize) {
            return Lists.concat(s1.records, s2.records);
        }
        // https://ballsandbins.wordpress.com/2014/04/13/distributedparallel-reservoir-sampling/
        int s1Size = s1.records.size();
        int s2Size = s2.records.size();
        ArrayList<Row> newSamples = new ArrayList<>(maxSampleSize);
        double p = (double) s2Size / (s2Size + s1Size);
        for (int i = 0; i < maxSampleSize; i++) {
            double j = random.nextDouble();
            if (j <= p) {
                newSamples.add(s1.records.get(random.nextInt(s1Size)));
            } else {
                newSamples.add(s2.records.get(random.nextInt(s2Size)));
            }
        }
        return newSamples;
    }
}

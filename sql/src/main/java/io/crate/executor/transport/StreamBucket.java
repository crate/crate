/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class StreamBucket implements Bucket, Streamable {

    private Streamer<?>[] streamers;
    private int size = -1;
    private BytesReference bytes;

    public static class Builder {


        private static final int INITIAL_PAGE_SIZE = 1024;
        private int size = 0;
        private final Streamer<?>[] streamers;
        private final BytesStreamOutput out;

        public static final Function<Builder, Bucket> BUILD_FUNCTION =
                new Function<Builder, Bucket>() {
                    @Nullable
                    @Override
                    public Bucket apply(StreamBucket.Builder input) {
                        try {
                            return input.build();
                        } catch (IOException e) {
                            Throwables.propagate(e);
                        }
                        return null;
                    }
                };

        public Builder(Streamer<?>[] streamers) {
            this.streamers = streamers;
            this.out = new BytesStreamOutput(INITIAL_PAGE_SIZE);
        }

        public void add(Row row) throws IOException {
            size++;
            for (int i = 0; i < row.size(); i++) {
                streamers[i].writeValueTo(out, row.get(i));
            }
        }

        public void writeToStream(StreamOutput output) throws IOException {
            output.writeVInt(size);
            if (size > 0) {
                output.writeBytesReference(out.bytes());
            }
        }

        public StreamBucket build() throws IOException {
            StreamBucket sb = new StreamBucket(streamers);
            sb.size = size;
            sb.bytes = out.bytes();
            return sb;
        }
    }

    public StreamBucket(@Nullable Streamer<?>[] streamers) {
        this.streamers = streamers;
    }

    @Override
    public int size() {
        return size;
    }

    public void streamers(Streamer<?>[] streamers) {
        this.streamers = streamers;
    }

    public static void writeBucket(StreamOutput out, @Nullable Streamer<?>[] streamers, @Nullable Bucket bucket) throws IOException {
        if (bucket == null || bucket.size() == 0) {
            out.writeVInt(0);
        } else if (bucket instanceof Streamable) {
            ((Streamable) bucket).writeTo(out);
        } else {
            StreamBucket.Builder builder = new StreamBucket.Builder(streamers);
            for (Row row : bucket) {
                builder.add(row);
            }
            builder.writeToStream(out);
        }
    }

    private class RowIterator implements Iterator<Row> {

        private final StreamInput input = bytes.streamInput();
        private int pos = 0;
        private final Object[] current = new Object[streamers.length];
        private final Row row = new Row() {
            @Override
            public int size() {
                return current.length;
            }

            @Override
            public Object get(int index) {
                return current[index];
            }

            @Override
            public Object[] materialize() {
                return Buckets.materialize(this);
            }

            @Override
            public String toString() {
                return Arrays.toString(current);
            }
        };

        @Override
        public boolean hasNext() {
            return pos < size;
        }

        @Override
        public Row next() {
            for (int c = 0; c < streamers.length; c++) {
                try {
                    current[c] = streamers[c].readValueFrom(input);
                } catch (IOException e) {
                    Throwables.propagate(e);
                }
            }
            pos++;
            return row;
        }

        @Override
        public void remove() {

        }
    }

    @Override
    public Iterator<Row> iterator() {
        if (size < 1) {
            return Collections.emptyIterator();
        }
        assert streamers != null;
        return new RowIterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        size = in.readVInt();
        if (size > 0) {
            bytes = in.readBytesReference();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert size > -1;
        out.writeVInt(size);
        if (size > 0) {
            out.writeBytesReference(bytes);
        }
    }
}

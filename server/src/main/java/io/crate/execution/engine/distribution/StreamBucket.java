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

package io.crate.execution.engine.distribution;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.breaker.RamAccounting;

public class StreamBucket implements Bucket, Writeable {

    private Streamer<?>[] streamers;
    private int size = -1;
    private BytesReference bytes;

    public static class Builder implements Accountable {

        private static final int INITIAL_PAGE_SIZE = 1024;
        private final RamAccounting ramAccounting;
        private final Streamer<?>[] streamers;

        private int size = 0;
        private BytesStreamOutput out;
        private int prevOutSize = 0;

        public Builder(Streamer<?>[] streamers, RamAccounting ramAccounting) {
            this.ramAccounting = requireNonNull(ramAccounting, "RamAccounting must not be null");
            assert validStreamers(streamers) : "streamers must not be null and they shouldn't be of undefinedType";
            this.streamers = streamers;
            out = new BytesStreamOutput(INITIAL_PAGE_SIZE);
        }

        public void add(Row row) {
            assert streamers.length == row.numColumns() : "number of streamer must match row size";

            size++;
            for (int i = 0; i < row.numColumns(); i++) {
                try {
                    //noinspection unchecked
                    ((Streamer) streamers[i]).writeValueTo(out, row.get(i));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            ramAccounting.addBytes(out.size() - prevOutSize);
            prevOutSize = out.size();
        }

        public StreamBucket build() {
            StreamBucket sb = new StreamBucket(streamers);
            sb.size = size;
            sb.bytes = out.bytes();
            return sb;
        }

        public void reset() {
            out = new BytesStreamOutput(size); // next bucket is probably going to have the same size
            size = 0;
        }

        public int size() {
            return size;
        }

        @Override
        public long ramBytesUsed() {
            return ramAccounting.totalBytes();
        }
    }

    public StreamBucket(@Nullable Streamer<?>[] streamers) {
        assert validStreamers(streamers) : "streamers must not be null and they shouldn't be of undefinedType";
        this.streamers = streamers;
    }

    @Override
    public int size() {
        return size;
    }

    public void streamers(Streamer<?>[] streamers) {
        assert validStreamers(streamers) : "streamers must not be null and they shouldn't be of undefinedType";
        this.streamers = streamers;
    }

    private static boolean validStreamers(Streamer<?>[] streamers) {
        if (streamers == null) {
            return true;
        }
        for (Streamer<?> streamer : streamers) {
            if (streamer == null) {
                return false;
            }
        }
        return true;
    }

    private static class RowIterator implements Iterator<Row> {

        private final Streamer<?>[] streamers;
        private final int size;
        private final StreamInput input;
        private final Object[] current;
        private final RowN row;
        private int pos = 0;

        private RowIterator(StreamInput streamInput, Streamer<?>[] streamers, int size) {
            this.streamers = streamers;
            this.size = size;
            input = streamInput;
            current = new Object[streamers.length];
            row = new RowN(current);
        }

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
                    throw new UncheckedIOException(e);
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
    @NotNull
    public Iterator<Row> iterator() {
        if (size < 1) {
            return Collections.emptyIterator();
        }
        assert streamers != null : "streamers must not be null";
        try {
            return new RowIterator(bytes.streamInput(), streamers, size);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Create a StreamBucket by reading from an input stream.
     * The created buckets rows are lazily de-serialized using the provided streamers
     */
    public StreamBucket(StreamInput in, Streamer<?>[] streamers) throws IOException {
        this(in);
        this.streamers = streamers;
    }

    /**
     * Create a StreamBucket by reading from an input stream.
     * Before consuming the rows it is necessary to set the {@link #streamers(Streamer[])}
     */
    public StreamBucket(StreamInput in) throws IOException {
        size = in.readVInt();
        if (size > 0) {
            bytes = in.readBytesReference();
        }
    }

    @Override
    public String toString() {
        return "StreamBucket{" +
               "size=" + size +
               '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert size > -1 : "size must be > -1";
        out.writeVInt(size);
        if (size > 0) {
            out.writeBytesReference(bytes);
        }
    }
}

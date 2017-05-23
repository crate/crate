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

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Bucket;
import io.crate.data.Row;
import io.crate.data.RowN;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

public class StreamBucket implements Bucket, Streamable {

    private Streamer<?>[] streamers;
    private int size = -1;
    private BytesReference bytes;

    public static class Builder {


        private static final int INITIAL_PAGE_SIZE = 1024;
        private final RamAccountingContext ramAccountingContext;

        private int size = 0;
        private final Streamer<?>[] streamers;
        private BytesStreamOutput out;
        private int prevOutSize = 0;

        public Builder(Streamer<?>[] streamers, RamAccountingContext ramAccountingContext) {
            this.ramAccountingContext = ramAccountingContext;
            assert validStreamers(streamers) : "streamers must not be null and they shouldn't be of undefinedType";
            this.streamers = streamers;
            out = new BytesStreamOutput(INITIAL_PAGE_SIZE);
        }

        public void add(Row row) throws IOException {
            assert streamers.length == row.numColumns() : "number of streamer must match row size";

            size++;
            for (int i = 0; i < row.numColumns(); i++) {
                streamers[i].writeValueTo(out, row.get(i));
            }
            if (ramAccountingContext != null) {
                ramAccountingContext.addBytes(out.size() - prevOutSize);
                prevOutSize = out.size();
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

        public void reset() {
            out = new BytesStreamOutput(size); // next bucket is probably going to have the same size
            size = 0;
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
        if (streamers == null || streamers.length == 0) {
            return true;
        }
        return !Iterables.all(FluentIterable.of(streamers), Predicates.isNull());
    }

    public static void writeBucket(StreamOutput out, @Nullable Streamer<?>[] streamers, @Nullable Bucket bucket) throws IOException {
        if (bucket == null || bucket.size() == 0) {
            out.writeVInt(0);
        } else if (bucket instanceof Streamable) {
            ((Streamable) bucket).writeTo(out);
        } else {
            assert streamers != null : "Need streamers for non-streamable bucket implementation";
            StreamBucket.Builder builder = new StreamBucket.Builder(streamers, null);
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
        private final Row row = new RowN(current);

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
        assert streamers != null : "streamers must not be null";
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
        assert size > -1 : "size must be > -1";
        out.writeVInt(size);
        if (size > 0) {
            out.writeBytesReference(bytes);
        }
    }
}

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

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.datasketches.common.ArrayOfItemsSerDe;
import org.apache.datasketches.memory.Buffer;
import org.apache.datasketches.memory.Memory;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.Streamer;

class SketchStreamer<T> extends ArrayOfItemsSerDe<T> {

    private final Streamer<T> streamer;

    public SketchStreamer(Streamer<T> streamer) {
        this.streamer = streamer;
    }

    @Override
    public byte[] serializeToByteArray(T item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] serializeToByteArray(T[] items) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (var v : items) {
                streamer.writeValueTo(out, v);
            }
            return out.bytes().toBytesRef().bytes;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public T[] deserializeFromMemory(Memory mem, int numItems) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public T[] deserializeFromMemory(Memory mem, long offsetBytes, int numItems) {
        try (MemoryStreamInput in = new MemoryStreamInput(mem)) {
            List<T> values = new ArrayList<>();
            for (int i = 0; i < numItems; i++) {
                values.add(streamer.readValueFrom(in));
            }
            return (T[]) values.toArray(Object[]::new);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int sizeOf(T item) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int sizeOf(Memory mem, long offsetBytes, int numItems) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString(T item) {
        return item.toString();
    }

    @Override
    public Class<?> getClassOfT() {
        throw new UnsupportedOperationException();
    }

    private static class MemoryStreamInput extends StreamInput {

        final Buffer buffer;

        private MemoryStreamInput(Memory memory) {
            this.buffer = memory.asBuffer();
        }

        @Override
        public byte readByte() throws IOException {
            return this.buffer.getByte();
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            this.buffer.getByteArray(b, offset, len);
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public int read() throws IOException {
            return this.buffer.getInt();
        }

        @Override
        public int available() throws IOException {
            return 0;
        }

        @Override
        protected void ensureCanReadBytes(int length) throws EOFException {

        }
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.io.IOUtils;

/**
 * Simple utility methods for file and stream copying.
 * All copy methods use a block size of 4096 bytes,
 * and close all affected streams when done.
 * <p>
 * Mainly for use within the framework,
 * but also useful for application code.
 */
public abstract class Streams {

    public static final int BUFFER_SIZE = 1024 * 8;

    /**
     * OutputStream that just throws all the bytes away
     */
    public static final OutputStream NULL_OUTPUT_STREAM = new OutputStream() {

        @Override
        public void write(int b) throws IOException {
            // no-op
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            // no-op
        }
    };

    /**
     * Copy the contents of the given InputStream to the given OutputStream.
     * Closes both streams when done.
     *
     * @param in  the stream to copy from
     * @param out the stream to copy to
     * @return the number of bytes copied
     * @throws IOException in case of I/O errors
     */
    public static long copy(InputStream in, OutputStream out, byte[] buffer) throws IOException {
        Objects.requireNonNull(in, "No InputStream specified");
        Objects.requireNonNull(out, "No OutputStream specified");
        boolean success = false;
        try {
            long byteCount = 0;
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                byteCount += bytesRead;
            }
            out.flush();
            success = true;
            return byteCount;
        } finally {
            if (success) {
                IOUtils.close(in, out);
            } else {
                IOUtils.closeWhileHandlingException(in, out);
            }
        }
    }

    public static int readFully(InputStream reader, byte[] dest) throws IOException {
        return readFully(reader, dest, 0, dest.length);
    }

    public static int readFully(InputStream reader, byte[] dest, int offset, int len) throws IOException {
        int read = 0;
        while (read < len) {
            final int r = reader.read(dest, offset + read, len - read);
            if (r == -1) {
                break;
            }
            read += r;
        }
        return read;
    }

    /**
     * Reads all bytes from the given {@link InputStream} and closes it afterwards.
     */
    public static BytesReference readFully(InputStream in) throws IOException {
        try (InputStream inputStream = in) {
            BytesStreamOutput out = new BytesStreamOutput();
            io.crate.common.io.Streams.copy(inputStream, out);
            return out.bytes();
        }
    }

    /**
     * Fully consumes the input stream, throwing the bytes away. Returns the number of bytes consumed.
     */
    public static long consumeFully(InputStream inputStream) throws IOException {
        return io.crate.common.io.Streams.copy(inputStream, NULL_OUTPUT_STREAM);
    }

    /**
     * Limits the given input stream to the provided number of bytes
     */
    public static InputStream limitStream(InputStream in, long limit) {
        return new LimitedInputStream(in, limit);
    }

    /**
     * Wraps the given {@link BytesStream} in a {@link StreamOutput} that simply flushes when
     * close is called.
     */
    public static BytesStream flushOnCloseStream(BytesStream os) {
        return new FlushOnCloseOutputStream(os);
    }

    /**
     * A wrapper around a {@link BytesStream} that makes the close operation a flush. This is
     * needed as sometimes a stream will be closed but the bytes that the stream holds still need
     * to be used and the stream cannot be closed until the bytes have been consumed.
     */
    private static class FlushOnCloseOutputStream extends BytesStream {

        private final BytesStream delegate;

        private FlushOnCloseOutputStream(BytesStream bytesStreamOutput) {
            this.delegate = bytesStreamOutput;
        }

        @Override
        public void writeByte(byte b) throws IOException {
            delegate.writeByte(b);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            delegate.writeBytes(b, offset, length);
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
        }

        @Override
        public void reset() throws IOException {
            delegate.reset();
        }

        @Override
        public BytesReference bytes() {
            return delegate.bytes();
        }
    }

    /**
     * A wrapper around an {@link InputStream} that limits the number of bytes that can be read from the stream.
     */
    static class LimitedInputStream extends FilterInputStream {

        private static final long NO_MARK = -1L;

        private long currentLimit; // is always non-negative
        private long limitOnLastMark;

        LimitedInputStream(InputStream in, long limit) {
            super(in);
            if (limit < 0L) {
                throw new IllegalArgumentException("limit must be non-negative");
            }
            this.currentLimit = limit;
            this.limitOnLastMark = NO_MARK;
        }

        @Override
        public int read() throws IOException {
            final int result;
            if (currentLimit == 0 || (result = in.read()) == -1) {
                return -1;
            } else {
                currentLimit--;
                return result;
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            final int result;
            if (currentLimit == 0 || (result = in.read(b, off, Math.toIntExact(Math.min(len, currentLimit)))) == -1) {
                return -1;
            } else {
                currentLimit -= result;
                return result;
            }
        }

        @Override
        public long skip(long n) throws IOException {
            final long skipped = in.skip(Math.min(n, currentLimit));
            currentLimit -= skipped;
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return Math.toIntExact(Math.min(in.available(), currentLimit));
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            in.mark(readlimit);
            limitOnLastMark = currentLimit;
        }

        @Override
        public synchronized void reset() throws IOException {
            in.reset();
            if (limitOnLastMark != NO_MARK) {
                currentLimit = limitOnLastMark;
            }
        }
    }
}

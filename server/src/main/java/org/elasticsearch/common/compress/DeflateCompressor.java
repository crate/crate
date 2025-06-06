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

package org.elasticsearch.common.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.util.zip.InflaterOutputStream;

import org.elasticsearch.Assertions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;

/**
 * {@link Compressor} implementation based on the DEFLATE compression algorithm.
 */
public class DeflateCompressor implements Compressor {

    // An arbitrary header that we use to identify compressed streams
    // It needs to be different from other compressors and to not be specific
    // enough so that no stream starting with these bytes could be detected as
    // a XContent
    private static final byte[] HEADER = new byte[]{'D', 'F', 'L', '\0'};
    // 3 is a good trade-off between speed and compression ratio
    private static final int LEVEL = 3;
    // We use buffering on the input and output of in/def-laters in order to
    // limit the number of JNI calls
    private static final int BUFFER_SIZE = 4096;

    @Override
    public boolean isCompressed(BytesReference bytes) {
        if (bytes.length() < HEADER.length) {
            return false;
        }
        for (int i = 0; i < HEADER.length; ++i) {
            if (bytes.get(i) != HEADER[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int headerLength() {
        return HEADER.length;
    }

    // Reusable inflater reference for streaming decompression
    private static final ThreadLocal<ReleasableReference<Inflater>> INFLATER_FOR_STREAM_REF = ThreadLocal.withInitial(() -> {
        final Inflater inflater = new Inflater(true);
        return new ReleasableReference<>(inflater, inflater::reset);
    });

    // Reusable deflater reference for streaming compression
    private static final ThreadLocal<ReleasableReference<Deflater>> DEFLATER_FOR_STREAM_REF = ThreadLocal.withInitial(() -> {
        final Deflater deflater = new Deflater(LEVEL, true);
        return new ReleasableReference<>(deflater, deflater::reset);
    });

    // Reference to a deflater or inflater that is used to make sure we do not use the same stream twice when nesting streams.
    private static final class ReleasableReference<T> implements Releasable {

        private final T resource;

        private final Releasable releasable;

        // Thread that is currently using this reference. Only used for assertions and only assigned if assertions are enabled.
        private Thread thread = null;

        // true if this reference is currently in use and is not available for re-use
        boolean inUse;

        private ReleasableReference(T resource, Releasable releasable) {
            this.resource = resource;
            this.releasable = releasable;
        }

        T get() {
            if (Assertions.ENABLED) {
                assert thread == null;
                thread = Thread.currentThread();
            }
            assert inUse == false;
            inUse = true;
            return resource;
        }

        @Override
        public void close() {
            if (Assertions.ENABLED) {
                assert thread == Thread.currentThread() :
                        "Opened on [" + thread.getName() + "] but closed on [" + Thread.currentThread().getName() + "]";
                thread = null;
            }
            assert inUse;
            inUse = false;
            releasable.close();
        }
    }

    @Override
    public InputStream threadLocalInputStream(InputStream in) throws IOException {
        return inputStream(in, true);
    }

    /**
     * Creates a new input stream that decompresses the contents read from the provided input stream.
     * Closing the returned stream will close the provided input stream.
     * Optionally uses thread-local, pooled resources to save off-heap allocations if the stream is guaranteed to not escape the current
     * thread.
     *
     * @param in           input stream to wrap
     * @param threadLocal  whether this stream will only be used on the current thread or not
     * @return             decompressing stream
     */
    public static InputStream inputStream(InputStream in, boolean threadLocal) throws IOException {
        final byte[] headerBytes = new byte[HEADER.length];
        int len = 0;
        while (len < headerBytes.length) {
            final int read = in.read(headerBytes, len, headerBytes.length - len);
            if (read == -1) {
                break;
            }
            len += read;
        }
        if (len != HEADER.length || Arrays.equals(headerBytes, HEADER) == false) {
            throw new IllegalArgumentException("Input stream is not compressed with DEFLATE!");
        }

        final Releasable releasable;
        final Inflater inflater;
        if (threadLocal) {
            final ReleasableReference<Inflater> current = INFLATER_FOR_STREAM_REF.get();
            if (current.inUse) {
                // Nested de-compression streams should not happen but we still handle them safely by using a fresh Inflater
                inflater = new Inflater(true);
                releasable = inflater::end;
            } else {
                inflater = current.get();
                releasable = current;
            }
        } else {
            inflater = new Inflater(true);
            releasable = inflater::end;
        }
        return new BufferedInputStream(new InflaterInputStream(in, inflater, BUFFER_SIZE) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    // We are ensured to only call this once since we wrap this stream in a BufferedInputStream that will only close
                    // its delegate once
                    releasable.close();
                }
            }
        }, BUFFER_SIZE);
    }

    @Override
    public OutputStream threadLocalOutputStream(OutputStream out) throws IOException {
        out.write(HEADER);
        final ReleasableReference<Deflater> current = DEFLATER_FOR_STREAM_REF.get();
        final Releasable releasable;
        final Deflater deflater;
        if (current.inUse) {
            // Nested compression streams should not happen but we still handle them safely by using a fresh Deflater
            deflater = new Deflater(LEVEL, true);
            releasable = deflater::end;
        } else {
            deflater = current.get();
            releasable = current;
        }
        final boolean syncFlush = true;
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(out, deflater, BUFFER_SIZE, syncFlush);
        return new BufferedOutputStream(deflaterOutputStream, BUFFER_SIZE) {

            // Due to https://bugs.openjdk.java.net/browse/JDK-8054565 we can't rely on the buffered output stream to only close once
            // in code that has to support Java 8 so we manually manage a close flag for this stream.
            private boolean closed = false;

            public void close() throws IOException {
                if (closed) {
                    return;
                }
                closed = true;
                try {
                    super.close();
                } finally {
                    // important to release native memory
                    releasable.close();
                }
            }
        };
    }

    private static final ThreadLocal<BytesStreamOutput> BAOS = ThreadLocal.withInitial(BytesStreamOutput::new);

    // Reusable Inflater reference. Note: This is a separate instance from the one used for the decompressing stream wrapper because we
    // want to be able to deal with decompressing bytes references that were read from a decompressing stream.
    private static final ThreadLocal<Inflater> INFLATER_REF = ThreadLocal.withInitial(() -> new Inflater(true));

    @Override
    public BytesReference uncompress(BytesReference bytesReference) {
        final BytesStreamOutput buffer = BAOS.get();
        final Inflater inflater = INFLATER_REF.get();
        try (InflaterOutputStream ios = new InflaterOutputStream(buffer, inflater)) {
            bytesReference.slice(HEADER.length, bytesReference.length() - HEADER.length).writeTo(ios);
        } catch (IOException e) {
            // we're operating on a BytesStreamOutput so this should never happen!
            throw new UncheckedIOException(e);
        } finally {
            inflater.reset();
        }
        final BytesReference res = buffer.copyBytes();
        buffer.reset();
        return res;
    }

    // Reusable Deflater reference. Note: This is a separate instance from the one used for the compressing stream wrapper because we
    // want to be able to deal with compressing bytes references to a decompressing stream.
    private static final ThreadLocal<Deflater> DEFLATER_REF = ThreadLocal.withInitial(() -> new Deflater(LEVEL, true));

    @Override
    public BytesReference compress(BytesReference bytesReference) {
        final BytesStreamOutput buffer = BAOS.get();
        try {
            buffer.write(HEADER);
            final Deflater deflater = DEFLATER_REF.get();
            try (DeflaterOutputStream dos = new DeflaterOutputStream(buffer, deflater, true)) {
                bytesReference.writeTo(dos);
            } finally {
                deflater.reset();
            }
            final BytesReference res = buffer.copyBytes();
            buffer.reset();
            return res;
        } catch (IOException e) {
            // We're working on a BytesStreamOutput so this should never happen
            throw new UncheckedIOException(e);
        }
    }
}

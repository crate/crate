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

package org.elasticsearch.transport;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.EOFException;
import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

public class CompressibleBytesOutputStreamTests extends ESTestCase {

    public void testStreamWithoutCompression() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, false);

        byte[] expectedBytes = randomBytes(randomInt(30));
        stream.write(expectedBytes);

        BytesReference bytesRef = stream.materializeBytes();
        // Closing compression stream does not close underlying stream
        stream.close();

        assertThat(CompressorFactory.COMPRESSOR.isCompressed(bytesRef)).isFalse();

        StreamInput streamInput = bytesRef.streamInput();
        byte[] actualBytes = new byte[expectedBytes.length];
        streamInput.readBytes(actualBytes, 0, expectedBytes.length);
        assertThat(streamInput.read()).isEqualTo(-1);
        assertThat(actualBytes).isEqualTo(expectedBytes);

        bStream.close();

        // The bytes should be zeroed out on close
        for (byte b : bytesRef.toBytesRef().bytes) {
            assertThat(b).isEqualTo((byte) 0);
        }
    }

    public void testStreamWithCompression() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, true);

        byte[] expectedBytes = randomBytes(randomInt(30));
        stream.write(expectedBytes);

        BytesReference bytesRef = stream.materializeBytes();
        stream.close();

        assertThat(CompressorFactory.COMPRESSOR.isCompressed(bytesRef)).isTrue();

        StreamInput streamInput = new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(bytesRef.streamInput()));
        byte[] actualBytes = new byte[expectedBytes.length];
        streamInput.readBytes(actualBytes, 0, expectedBytes.length);

        assertThat(streamInput.read()).isEqualTo(-1);
        assertThat(actualBytes).isEqualTo(expectedBytes);

        bStream.close();

        // The bytes should be zeroed out on close
        for (byte b : bytesRef.toBytesRef().bytes) {
            assertThat(b).isEqualTo((byte) 0);
        }
    }

    public void testCompressionWithCallingMaterializeFails() throws IOException {
        BytesStream bStream = new ZeroOutOnCloseStream();
        CompressibleBytesOutputStream stream = new CompressibleBytesOutputStream(bStream, true);

        byte[] expectedBytes = randomBytes(between(1, 30));
        stream.write(expectedBytes);


        StreamInput streamInput =
                new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(bStream.bytes().streamInput()));
        byte[] actualBytes = new byte[expectedBytes.length];
        Assertions.assertThatThrownBy(() -> streamInput.readBytes(actualBytes, 0, expectedBytes.length))
            .isExactlyInstanceOf(EOFException.class)
            .hasMessageContaining("Unexpected end of ZLIB input stream");
        stream.close();
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    private static class ZeroOutOnCloseStream extends BytesStreamOutput {

        @Override
        public void close() {
            if (bytes != null) {
                int size = (int) bytes.size();
                bytes.set(0, new byte[size], 0, size);
            }
        }
    }
}

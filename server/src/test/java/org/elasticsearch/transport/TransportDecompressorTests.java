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

import java.io.IOException;
import java.io.OutputStream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;

public class TransportDecompressorTests extends ESTestCase {

    public void testSimpleCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            byte randomByte = randomByte();
            try (OutputStream deflateStream = CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(output))) {
                deflateStream.write(randomByte);
            }

            BytesReference bytes = output.bytes();

            TransportDecompressor decompressor = new TransportDecompressor(PageCacheRecycler.NON_RECYCLING_INSTANCE);
            int bytesConsumed = decompressor.decompress(bytes);
            assertThat(bytesConsumed).isEqualTo(bytes.length());
            assertThat(decompressor.isEOS()).isTrue();
            ReleasableBytesReference releasableBytesReference = decompressor.pollDecompressedPage();
            assertThat(releasableBytesReference.get(0)).isEqualTo(randomByte);
            releasableBytesReference.close();

        }
    }

    public void testMultiPageCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (StreamOutput deflateStream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(
                    Streams.flushOnCloseStream(output)))) {
                for (int i = 0; i < 10000; ++i) {
                    deflateStream.writeInt(i);
                }
            }

            BytesReference bytes = output.bytes();

            TransportDecompressor decompressor = new TransportDecompressor(PageCacheRecycler.NON_RECYCLING_INSTANCE);
            int bytesConsumed = decompressor.decompress(bytes);
            assertThat(bytesConsumed).isEqualTo(bytes.length());
            assertThat(decompressor.isEOS()).isTrue();
            ReleasableBytesReference reference1 = decompressor.pollDecompressedPage();
            ReleasableBytesReference reference2 = decompressor.pollDecompressedPage();
            ReleasableBytesReference reference3 = decompressor.pollDecompressedPage();
            assertThat(decompressor.pollDecompressedPage()).isNull();
            BytesReference composite = CompositeBytesReference.of(reference1, reference2, reference3);
            assertThat(composite.length()).isEqualTo(4 * 10000);
            StreamInput streamInput = composite.streamInput();
            for (int i = 0; i < 10000; ++i) {
                assertThat(streamInput.readInt()).isEqualTo(i);
            }
            Releasables.close(reference1, reference2, reference3);
        }
    }

    public void testIncrementalMultiPageCompression() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            try (StreamOutput deflateStream = new OutputStreamStreamOutput(
                    CompressorFactory.COMPRESSOR.threadLocalOutputStream(Streams.flushOnCloseStream(output)))) {
                for (int i = 0; i < 10000; ++i) {
                    deflateStream.writeInt(i);
                }
            }

            BytesReference bytes = output.bytes();

            TransportDecompressor decompressor = new TransportDecompressor(PageCacheRecycler.NON_RECYCLING_INSTANCE);

            int split1 = (int) (bytes.length() * 0.3);
            int split2 = (int) (bytes.length() * 0.65);
            BytesReference inbound1 = bytes.slice(0, split1);
            BytesReference inbound2 = bytes.slice(split1, split2 - split1);
            BytesReference inbound3 = bytes.slice(split2, bytes.length() - split2);

            int bytesConsumed1 = decompressor.decompress(inbound1);
            assertThat(bytesConsumed1).isEqualTo(inbound1.length());
            assertThat(decompressor.isEOS()).isFalse();
            int bytesConsumed2 = decompressor.decompress(inbound2);
            assertThat(bytesConsumed2).isEqualTo(inbound2.length());
            assertThat(decompressor.isEOS()).isFalse();
            int bytesConsumed3 = decompressor.decompress(inbound3);
            assertThat(bytesConsumed3).isEqualTo(inbound3.length());
            assertThat(decompressor.isEOS()).isTrue();
            ReleasableBytesReference reference1 = decompressor.pollDecompressedPage();
            ReleasableBytesReference reference2 = decompressor.pollDecompressedPage();
            ReleasableBytesReference reference3 = decompressor.pollDecompressedPage();
            assertThat(decompressor.pollDecompressedPage()).isNull();
            BytesReference composite = CompositeBytesReference.of(reference1, reference2, reference3);
            assertThat(composite.length()).isEqualTo(4 * 10000);
            StreamInput streamInput = composite.streamInput();
            for (int i = 0; i < 10000; ++i) {
                assertThat(streamInput.readInt()).isEqualTo(i);
            }
            Releasables.close(reference1, reference2, reference3);

        }
    }

}

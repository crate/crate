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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;


public class DeflateCompressedXContentTests extends ESTestCase {

    private final Compressor compressor = new DeflateCompressor();

    private void assertEquals(CompressedXContent s1, CompressedXContent s2) {
        assertThat(s1).isEqualTo(s2);
        assertThat(s1.uncompressed()).isEqualTo(s2.uncompressed());
        assertThat(s1.hashCode()).isEqualTo(s2.hashCode());
    }

    public void simpleTests() throws IOException {
        String str = "---\nf:this is a simple string";
        CompressedXContent cstr = new CompressedXContent(str);
        assertThat(cstr.string()).isEqualTo(str);
        assertThat(new CompressedXContent(str)).isEqualTo(cstr);

        String str2 = "---\nf:this is a simple string 2";
        CompressedXContent cstr2 = new CompressedXContent(str2);
        assertThat(cstr2.string()).isNotEqualTo(str);
        assertThat(new CompressedXContent(str2)).isNotEqualTo(cstr);
        assertEquals(new CompressedXContent(str2), cstr2);
    }

    public void testRandom() throws IOException {
        Random r = random();
        for (int i = 0; i < 1000; i++) {
            String string = RandomStrings.randomUnicodeOfLength(r, 10000);
            // hack to make it detected as YAML
            string = "---\n" + string;
            CompressedXContent compressedXContent = new CompressedXContent(string);
            assertThat(compressedXContent.string()).isEqualTo(string);
        }
    }

    public void testDifferentCompressedRepresentation() throws Exception {
        byte[] b = "---\nf:abcdefghijabcdefghij".getBytes("UTF-8");
        BytesStreamOutput bout = new BytesStreamOutput();
        try (OutputStream out = compressor.threadLocalOutputStream(bout)) {
            out.write(b);
            out.flush();
            out.write(b);
        }
        final BytesReference b1 = bout.bytes();

        bout = new BytesStreamOutput();
        try (OutputStream out = compressor.threadLocalOutputStream(bout)) {
            out.write(b);
            out.write(b);
        }
        final BytesReference b2 = bout.bytes();

        // because of the intermediate flush, the two compressed representations
        // are different. It can also happen for other reasons like if hash tables
        // of different size are being used
        assertThat(b1).isNotEqualTo(b2);
        // we used the compressed representation directly and did not recompress
        assertThat(BytesReference.toBytes(b1)).isEqualTo(new CompressedXContent(b1).compressed());
        assertThat(BytesReference.toBytes(b2)).isEqualTo(new CompressedXContent(b2).compressed());
        // but compressedstring instances are still equal
        assertEquals(new CompressedXContent(b1), new CompressedXContent(b2));
    }

    public void testHashCode() throws IOException {
        assertThat(new CompressedXContent("{\"a\":\"b\"}").hashCode()).isNotEqualTo(new CompressedXContent("{\"a\":\"c\"}").hashCode());
    }

}

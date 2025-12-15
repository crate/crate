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

import java.util.Objects;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.jspecify.annotations.Nullable;

import com.fasterxml.jackson.dataformat.smile.SmileConstants;

public class CompressorFactory {

    public static final Compressor COMPRESSOR = new DeflateCompressor();

    @Nullable
    public static Compressor compressor(BytesReference bytes) {
        if (COMPRESSOR.isCompressed(bytes)) {
            // bytes should be either detected as compressed or as xcontent,
            // if we have bytes that can be either detected as compressed or
            // as a xcontent, we have a problem
            assert xContentType(bytes) == null;
            return COMPRESSOR;
        }
        XContentType contentType = xContentType(bytes);
        if (contentType == null) {
            if (isAncient(bytes)) {
                throw new IllegalStateException(
                    "unsupported compression: index was created before v2.0.0.beta1 and wasn't upgraded?"
                );
            }
            throw new NotXContentException(
                "Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes"
            );
        }
        return null;
    }

    /**
     * Guesses the content type based on the provided bytes.
     *
     */
    private static XContentType xContentType(BytesReference bytesReference) {
        BytesRef br = bytesReference.toBytesRef();
        byte[] bytes = br.bytes;
        int offset = br.offset;
        int length = br.length;
        int totalLength = bytes.length;
        if (totalLength == 0 || length == 0) {
            return null;
        } else if ((offset + length) > totalLength) {
            return null;
        }
        byte first = bytes[offset];
        if (first == '{') {
            return XContentType.JSON;
        }
        if (length > 2
                && first == SmileConstants.HEADER_BYTE_1
                && bytes[offset + 1] == SmileConstants.HEADER_BYTE_2
                && bytes[offset + 2] == SmileConstants.HEADER_BYTE_3) {
            return XContentType.SMILE;
        }
        if (length > 2 && first == '-' && bytes[offset + 1] == '-' && bytes[offset + 2] == '-') {
            return XContentType.YAML;
        }

        int jsonStart = 0;
        // JSON may be preceded by UTF-8 BOM
        if (length > 3 && first == (byte) 0xEF && bytes[offset + 1] == (byte) 0xBB && bytes[offset + 2] == (byte) 0xBF) {
            jsonStart = 3;
        }

        // a last chance for JSON
        for (int i = jsonStart; i < length; i++) {
            byte b = bytes[offset + i];
            if (b == '{') {
                return XContentType.JSON;
            }
            if (Character.isWhitespace(b) == false) {
                break;
            }
        }
        return null;
    }

    /** true if the bytes were compressed with LZF: only used before elasticsearch 2.0 */
    private static boolean isAncient(BytesReference bytes) {
        return bytes.length() >= 3 &&
               bytes.get(0) == 'Z' &&
               bytes.get(1) == 'V' &&
               (bytes.get(2) == 0 || bytes.get(2) == 1);
    }

    /**
     * Uncompress the provided data
     * @throws NullPointerException a NullPointerException will be thrown when bytes is null
     */
    public static BytesReference uncompressIfNeeded(BytesReference bytes) {
        Compressor compressor = compressor(Objects.requireNonNull(bytes, "the BytesReference must not be null"));
        return compressor == null ? bytes : compressor.uncompress(bytes);
    }

    /** Decompress the provided {@link BytesReference}. */
    public static BytesReference uncompress(BytesReference bytes) {
        Compressor compressor = compressor(bytes);
        if (compressor == null) {
            throw new NotCompressedException();
        }
        return compressor.uncompress(bytes);
    }
}

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

package org.elasticsearch.repositories.blobstore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Locale;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.store.ByteArrayIndexInput;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.CorruptStateException;

import io.crate.common.CheckedFunction;
import io.crate.server.xcontent.LoggingDeprecationHandler;
import io.crate.server.xcontent.XContentHelper;

/**
 * Snapshot metadata file format used in v2.0 and above
 */
public final class ChecksumBlobStoreFormat<T extends Writeable> {

    // The format version
    public static final int XCONTENT_VERSION = 1;
    public static final int VERSION = 2;

    private static final int BUFFER_SIZE = 4096;

    private final String codec;

    private final String blobNameFormat;

    private final CheckedFunction<XContentParser, T, IOException> parse;
    private final Writeable.Reader<T> readFrom;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     */
    public ChecksumBlobStoreFormat(String codec,
                                   String blobNameFormat,
                                   CheckedFunction<XContentParser, T, IOException> parse,
                                   Writeable.Reader<T> readFrom) {
        this.blobNameFormat = blobNameFormat;
        this.codec = codec;
        this.parse = parse;
        this.readFrom = readFrom;
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     */
    public T read(BlobContainer blobContainer,
                  String name,
                  NamedWriteableRegistry namedWriteableRegistry,
                  NamedXContentRegistry namedXContentRegistry) throws IOException {
        String blobName = blobName(name);
        return deserialize(blobName, namedWriteableRegistry, namedXContentRegistry, Streams.readFully(blobContainer.readBlob(blobName)));
    }

    public String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    public T deserialize(String blobName,
                         NamedWriteableRegistry namedWritableRegistry,
                         NamedXContentRegistry namedXContentRegistry,
                         BytesReference bytes) throws IOException {
        final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
        try {
            final IndexInput indexInput = bytes.length() > 0 ? new ByteBuffersIndexInput(
                    new ByteBuffersDataInput(Arrays.asList(BytesReference.toByteBuffers(bytes))), resourceDesc)
                    : new ByteArrayIndexInput(resourceDesc, BytesRef.EMPTY_BYTES);
            CodecUtil.checksumEntireFile(indexInput);
            int stateVersion = CodecUtil.checkHeader(indexInput, codec, XCONTENT_VERSION, VERSION);
            long filePointer = indexInput.getFilePointer();
            long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;

            BytesReference slice = bytes.slice((int) filePointer, (int) contentSize);
            if (stateVersion == XCONTENT_VERSION) {
                try (XContentParser parser = XContentHelper.createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, slice, XContentType.SMILE)) {
                    return parse.apply(parser);
                }
            } else {
                StreamInput streamInput = CompressorFactory.COMPRESSOR.isCompressed(slice)
                    ? CompressorFactory.COMPRESSOR.uncompress(slice).streamInput()
                    : slice.streamInput();
                try (StreamInput in = new NamedWriteableAwareStreamInput(streamInput, namedWritableRegistry)) {
                    Version version = Version.readVersion(in);
                    in.setVersion(version);
                    T t = readFrom.read(in);
                    return t;
                }
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally by compressed.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compress            whether to use compression
     */
    public void write(T obj, BlobContainer blobContainer, String name, boolean compress) throws IOException {
        final String blobName = blobName(name);
        final BytesReference bytes = serialize(obj, blobName, compress);
        blobContainer.writeBlob(blobName, bytes.streamInput(), bytes.length(), false);
    }

    public BytesReference serialize(final T obj, final String blobName, boolean compress) throws IOException {
        try (BytesStreamOutput resultStream = new BytesStreamOutput()) {
            try (OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")", blobName, resultStream, BUFFER_SIZE)) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);
                OutputStream os = new IndexOutputOutputStream(indexOutput) {

                    // Prevents early indexOutput close
                    // - Must close the compression stream to ensure `obj` is fully written
                    // - Must not close indexOutput before writeFooter - it would be missing and result in checksum failures
                    public void close() throws IOException {
                    }
                };
                os = compress ? CompressorFactory.COMPRESSOR.threadLocalOutputStream(os) : os;
                try (var out = new OutputStreamStreamOutput(os)) {
                    Version.writeVersion(Version.CURRENT, out);
                    obj.writeTo(out);
                }
                CodecUtil.writeFooter(indexOutput);
            }
            return resultStream.bytes();
        }
    }
}

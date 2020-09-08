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

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.Compressor;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.compress.NotCompressedException;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import io.crate.common.io.IOUtils;

public abstract class InboundMessage extends NetworkMessage implements Closeable {

    private final StreamInput streamInput;

    InboundMessage(Version version, byte status, long requestId, StreamInput streamInput) {
        super(version, status, requestId);
        this.streamInput = streamInput;
    }

    StreamInput getStreamInput() {
        return streamInput;
    }

    static class Reader {

        private final Version version;
        private final NamedWriteableRegistry namedWriteableRegistry;

        Reader(Version version, NamedWriteableRegistry namedWriteableRegistry) {
            this.version = version;
            this.namedWriteableRegistry = namedWriteableRegistry;
        }

        InboundMessage deserialize(BytesReference reference) throws IOException {
            int messageLengthBytes = reference.length();
            final int totalMessageSize = messageLengthBytes + TcpHeader.MARKER_BYTES_SIZE + TcpHeader.MESSAGE_LENGTH_SIZE;
            // we have additional bytes to read, outside of the header
            boolean hasMessageBytesToRead = (totalMessageSize - TcpHeader.HEADER_SIZE) > 0;
            StreamInput streamInput = reference.streamInput();
            boolean success = false;
            try {
                long requestId = streamInput.readLong();
                byte status = streamInput.readByte();
                Version remoteVersion = Version.fromId(streamInput.readInt());
                final boolean isHandshake = TransportStatus.isHandshake(status);
                ensureVersionCompatibility(remoteVersion, version, isHandshake);
                if (TransportStatus.isCompress(status) && hasMessageBytesToRead && streamInput.available() > 0) {
                    Compressor compressor;
                    try {
                        final int bytesConsumed = TcpHeader.REQUEST_ID_SIZE + TcpHeader.STATUS_SIZE + TcpHeader.VERSION_ID_SIZE;
                        compressor = CompressorFactory.compressor(reference.slice(bytesConsumed, reference.length() - bytesConsumed));
                    } catch (NotCompressedException ex) {
                        int maxToRead = Math.min(reference.length(), 10);
                        StringBuilder sb = new StringBuilder("stream marked as compressed, but no compressor found, first [")
                            .append(maxToRead).append("] content bytes out of [").append(reference.length())
                            .append("] readable bytes with message size [").append(messageLengthBytes).append("] ").append("] are [");
                        for (int i = 0; i < maxToRead; i++) {
                            sb.append(reference.get(i)).append(",");
                        }
                        sb.append("]");
                        throw new IllegalStateException(sb.toString());
                    }
                    streamInput = compressor.streamInput(streamInput);
                }
                streamInput = new NamedWriteableAwareStreamInput(streamInput, namedWriteableRegistry);
                streamInput.setVersion(remoteVersion);

                ThreadContext.bwcReadHeaders(streamInput);

                InboundMessage message;
                if (TransportStatus.isRequest(status)) {
                    Set<String> features = Collections.unmodifiableSet(new TreeSet<>(Arrays.asList(streamInput.readStringArray())));
                    String action = streamInput.readString();
                    message = new Request(remoteVersion, status, requestId, action, features, streamInput);
                } else {
                    message = new Response(remoteVersion, status, requestId, streamInput);
                }
                success = true;
                return message;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(streamInput);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        streamInput.close();
    }

    private static void ensureVersionCompatibility(Version version, Version currentVersion, boolean isHandshake) {
        // for handshakes we are compatible with N-2 since otherwise we can't figure out our initial version
        // since we are compatible with N-1 and N+1 so we always send our minCompatVersion as the initial version in the
        // handshake. This looks odd but it's required to establish the connection correctly we check for real compatibility
        // once the connection is established
        final Version compatibilityVersion = isHandshake ? currentVersion.minimumCompatibilityVersion() : currentVersion;
        if (version.isCompatible(compatibilityVersion) == false) {
            final Version minCompatibilityVersion = isHandshake ? compatibilityVersion : compatibilityVersion.minimumCompatibilityVersion();
            String msg = "Received " + (isHandshake ? "handshake " : "") + "message from unsupported version: [";
            throw new IllegalStateException(msg + version + "] minimal compatible version is: [" + minCompatibilityVersion + "]");
        }
    }

    public static class Request extends InboundMessage {

        private final String actionName;
        private final Set<String> features;

        Request(Version version,
                       byte status,
                       long requestId,
                       String actionName,
                       Set<String> features,
                       StreamInput streamInput) {
            super(version, status, requestId, streamInput);
            this.actionName = actionName;
            this.features = features;
        }

        String getActionName() {
            return actionName;
        }

        Set<String> getFeatures() {
            return features;
        }
    }

    public static class Response extends InboundMessage {

        Response(Version version, byte status, long requestId, StreamInput streamInput) {
            super(version, status, requestId, streamInput);
        }
    }
}

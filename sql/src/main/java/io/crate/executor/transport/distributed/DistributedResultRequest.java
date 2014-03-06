/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.executor.transport.distributed;

import com.google.common.base.Optional;
import io.crate.Streamer;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;

public class DistributedResultRequest extends TransportRequest {

    private DistributedRequestContextManager contextManager;
    private Streamer<?>[] streamers;
    private Object[][] rows;
    private UUID contextId;
    private BytesStreamOutput memoryStream;

    // TODO: change failure flag to string or enum so that the receiver can recreate the
    // exception and the error handling in the DistributedMergeTask can be simplified.
    private boolean failure = false;

    public DistributedResultRequest(DistributedRequestContextManager contextManager) {
        this.contextManager = contextManager;
    }

    public DistributedResultRequest(UUID contextId, Streamer<?>[] streamers) {
        this.contextId = contextId;
        this.streamers = streamers;
    }

    public UUID contextId() {
        return contextId;
    }

    public BytesStreamOutput memoryStream() {
        return memoryStream;
    }

    public Object[][] rows() {
        return rows;
    }

    public void rows(Object[][] rows) {
        this.rows = rows;
    }

    public boolean rowsRead() {
        return memoryStream == null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        contextId = new UUID(in.readLong(), in.readLong());

        if (in.readBoolean()) {
            failure= true;
            return;
        }

        final Optional<Streamer<?>[]> optStreamer = contextManager.getStreamer(contextId);
        if (optStreamer.isPresent()) {
            final Streamer<?>[] streamers = optStreamer.get();
            final int numColumns = streamers.length;

            rows = new Object[in.readVInt()][];
            for (int r = 0; r < rows.length; r++) {
                rows[r] = new Object[numColumns];
                for (int c = 0; c < numColumns; c++) {
                    rows[r][c] = streamers[c].readFrom(in);
                }
            }
        } else {
            memoryStream = new BytesStreamOutput();
            Streams.copy(in, memoryStream);
        }
    }

    public static Object[][] readRemaining(Streamer<?>[] streamers, StreamInput input) throws IOException {
        final int numColumns = streamers.length;
        final Object[][] rows = new Object[input.readVInt()][];
        for (int r = 0; r < rows.length; r++) {
            rows[r] = new Object[numColumns];
            for (int c = 0; c < numColumns; c++) {
                rows[r][c] = streamers[c].readFrom(input);
            }
        }

        return rows;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());

        if (failure) {
            out.writeBoolean(true);
            return;
        }
        out.writeBoolean(false);

        assert streamers != null;
        final int numColumns = streamers.length;

        out.writeVInt(rows.length);
        for (Object[] row : rows) {
            for (int i = 0; i < numColumns; i++) {
                streamers[i].writeTo(out, row[i]);
            }
        }
    }

    public void failure(boolean failure) {
        this.failure = failure;
    }

    public boolean failure() {
        return this.failure;
    }
}

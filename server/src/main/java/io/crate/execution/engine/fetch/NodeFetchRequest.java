/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.fetch;

import java.io.IOException;
import java.util.UUID;

import javax.annotation.Nullable;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import io.crate.Streamer;
import io.crate.breaker.RamAccounting;
import io.crate.execution.support.NodeRequest;

public class NodeFetchRequest extends NodeRequest<NodeFetchRequest.FetchRequest> {

    private final Writeable.Reader<NodeFetchResponse> responseReader;

    public NodeFetchRequest(String nodeId,
                            UUID jobId,
                            int fetchPhaseId,
                            boolean closeContext,
                            IntObjectMap<IntArrayList> toFetch,
                            IntObjectMap<Streamer[]> streamers,
                            RamAccounting ramAccounting) {
        super(nodeId, new FetchRequest(jobId, fetchPhaseId, closeContext, toFetch));
        responseReader = in -> new NodeFetchResponse(in, streamers, ramAccounting);
    }

    public Writeable.Reader<NodeFetchResponse> createResponseReader() {
        return responseReader;
    }

    static class FetchRequest extends TransportRequest {

        private final UUID jobId;
        private final int fetchPhaseId;
        private final boolean closeContext;

        @Nullable
        private final IntObjectMap<IntArrayList> toFetch;

        private FetchRequest(UUID jobId,
                     int fetchPhaseId,
                     boolean closeContext,
                     IntObjectMap<IntArrayList> toFetch) {
            this.jobId = jobId;
            this.fetchPhaseId = fetchPhaseId;
            this.closeContext = closeContext;
            if (toFetch.isEmpty()) {
                this.toFetch = null;
            } else {
                this.toFetch = toFetch;
            }
        }

        public UUID jobId() {
            return jobId;
        }

        public int fetchPhaseId() {
            return fetchPhaseId;
        }

        public boolean isCloseContext() {
            return closeContext;
        }

        @Nullable
        public IntObjectMap<IntArrayList> toFetch() {
            return toFetch;
        }

        public FetchRequest(StreamInput in) throws IOException {
            super(in);
            jobId = new UUID(in.readLong(), in.readLong());
            fetchPhaseId = in.readVInt();
            closeContext = in.readBoolean();
            int numReaders = in.readVInt();
            if (numReaders > 0) {
                IntObjectHashMap<IntArrayList> toFetch = new IntObjectHashMap<>(numReaders);
                for (int i = 0; i < numReaders; i++) {
                    int readerId = in.readVInt();
                    int numDocs = in.readVInt();
                    IntArrayList docs = new IntArrayList(numDocs);
                    toFetch.put(readerId, docs);
                    for (int j = 0; j < numDocs; j++) {
                        docs.add(in.readInt());
                    }
                }
                this.toFetch = toFetch;
            } else {
                this.toFetch = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(jobId.getMostSignificantBits());
            out.writeLong(jobId.getLeastSignificantBits());
            out.writeVInt(fetchPhaseId);
            out.writeBoolean(closeContext);
            if (toFetch == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(toFetch.size());
                for (IntObjectCursor<? extends IntContainer> toFetchCursor : toFetch) {
                    out.writeVInt(toFetchCursor.key);
                    out.writeVInt(toFetchCursor.value.size());
                    for (IntCursor docCursor : toFetchCursor.value) {
                        out.writeInt(docCursor.value);
                    }
                }
            }
        }
    }
}

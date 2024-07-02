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
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;
import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.data.breaker.RamAccounting;
import io.crate.execution.engine.distribution.StreamBucket;

public class NodeFetchResponse extends TransportResponse {

    @Nullable
    private final IntObjectMap<StreamBucket> fetched;

    public NodeFetchResponse(@Nullable IntObjectMap<StreamBucket> fetched) {
        this.fetched = fetched;
    }

    @Nullable
    public IntObjectMap<? extends Bucket> fetched() {
        return fetched;
    }

    public NodeFetchResponse(StreamInput in, IntObjectMap<Streamer<?>[]> streamers, RamAccounting ramAccounting) throws IOException {
        ramAccounting.addBytes(in.available());
        int numReaders = in.readVInt();
        if (numReaders > 0) {
            assert streamers != null : "streamers must not be null";
            fetched = new IntObjectHashMap<>(numReaders);
            for (int i = 0; i < numReaders; i++) {
                int readerId = in.readVInt();
                StreamBucket bucket = new StreamBucket(in, Objects.requireNonNull(streamers.get(readerId), "streamers must exist for readerId=" + readerId));
                fetched.put(readerId, bucket);
            }
        } else {
            fetched = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (fetched == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(fetched.size());
            for (IntObjectCursor<StreamBucket> cursor : fetched) {
                out.writeVInt(cursor.key);
                cursor.value.writeTo(out);
            }
        }
    }
}

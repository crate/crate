/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.fetch;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.Streamer;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.Bucket;
import io.crate.execution.engine.distribution.StreamBucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import javax.annotation.Nullable;
import java.io.IOException;

public class NodeFetchResponse extends TransportResponse {

    private final IntObjectMap<Streamer[]> streamers;
    private final RamAccountingContext ramAccountingContext;

    @Nullable
    private IntObjectMap<StreamBucket> fetched;

    public static NodeFetchResponse forSending(IntObjectMap<StreamBucket> fetched) {
        return new NodeFetchResponse(null, fetched, null);
    }

    public static NodeFetchResponse forReceiveing(@Nullable IntObjectMap<Streamer[]> streamers,
                                                  RamAccountingContext ramAccountingContext) {
        return new NodeFetchResponse(streamers, null, ramAccountingContext);
    }

    private NodeFetchResponse(@Nullable IntObjectMap<Streamer[]> streamers,
                              @Nullable IntObjectMap<StreamBucket> fetched,
                              @Nullable RamAccountingContext ramAccountingContext) {
        this.streamers = streamers;
        this.fetched = fetched;
        this.ramAccountingContext = ramAccountingContext;
    }

    @Nullable
    public IntObjectMap<? extends Bucket> fetched() {
        return fetched;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        assert ramAccountingContext != null : "RamAccountingContext must be present to receive a NodeFetchResponse";
        ramAccountingContext.addBytes(in.available());

        super.readFrom(in);
        int numReaders = in.readVInt();
        if (numReaders > 0) {
            assert streamers != null : "streamers must not be null";
            fetched = new IntObjectHashMap<>(numReaders);
            for (int i = 0; i < numReaders; i++) {
                int readerId = in.readVInt();
                StreamBucket bucket = new StreamBucket(streamers.get(readerId));
                bucket.readFrom(in);
                fetched.put(readerId, bucket);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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

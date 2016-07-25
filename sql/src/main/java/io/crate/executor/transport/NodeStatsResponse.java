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

package io.crate.executor.transport;

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.Streamer;
import io.crate.analyze.symbol.Literal;
import io.crate.core.collections.Bucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodeStatsResponse extends TransportResponse {

    private List<Bucket> directResponse = new ArrayList<>();
    private Streamer<?>[] streamers = null;

    public NodeStatsResponse() {
    }

    public NodeStatsResponse(@Nonnull List<Bucket> buckets) {
        this.directResponse = buckets;
    }

    public List<Bucket> directResponse() {
        return directResponse;
    }

    public void streamers(Streamer<?>[] streamers) {
        List<Bucket> directResponse = directResponse();
        for (Bucket bucket : directResponse) {
            if (bucket instanceof StreamBucket) {
                assert streamers != null;
                ((StreamBucket) bucket).streamers(streamers);
            }
        }
        this.streamers = streamers;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            StreamBucket bucket = new StreamBucket(streamers);
            bucket.readFrom(in);
            directResponse.add(bucket);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(directResponse.size());
        for (Bucket bucket : directResponse) {
            StreamBucket.writeBucket(out, streamers, bucket);
        }
    }
}

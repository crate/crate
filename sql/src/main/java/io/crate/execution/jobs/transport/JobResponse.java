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

package io.crate.execution.jobs.transport;

import io.crate.Streamer;
import io.crate.data.Bucket;
import io.crate.execution.engine.distribution.StreamBucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JobResponse extends TransportResponse {

    private List<Bucket> directResponse = new ArrayList<>();
    private Streamer<?>[] streamers = null;

    public JobResponse() {
    }

    public JobResponse(@Nonnull List<Bucket> buckets) {
        this.directResponse = buckets;
    }

    public List<Bucket> directResponse() {
        return directResponse;
    }

    public void streamers(Streamer<?>[] streamers) {
        List<Bucket> directResponse = directResponse();
        for (Bucket bucket : directResponse) {
            if (bucket instanceof StreamBucket) {
                assert streamers != null : "streamers must not be null";
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

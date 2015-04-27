/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.action.job;

import com.google.common.base.Optional;
import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.StreamBucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import javax.annotation.Nonnull;
import java.io.IOException;

public class JobResponse extends TransportResponse {

    private Optional<Bucket> directResponse = Optional.absent();
    private Streamer<?>[] streamers = null;

    public JobResponse() {
    }

    public JobResponse(@Nonnull Bucket bucket) {
        this.directResponse = Optional.of(bucket);
    }

    public Optional<Bucket> directResponse() {
        return directResponse;
    }

    public void streamers(Streamer<?>[] streamers) {
        Bucket directResponse = directResponse().orNull();
        if (directResponse != null && directResponse instanceof StreamBucket) {
            assert streamers != null;
            ((StreamBucket) directResponse).streamers(streamers);
        }
        this.streamers = streamers;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            StreamBucket bucket = new StreamBucket(streamers);
            bucket.readFrom(in);
            directResponse = Optional.<Bucket>of(bucket);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(directResponse.isPresent());
        if (directResponse.isPresent()) {
            StreamBucket.writeBucket(out, streamers, directResponse.get());
        }
    }
}

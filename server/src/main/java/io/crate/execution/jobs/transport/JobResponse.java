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

package io.crate.execution.jobs.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import io.crate.Streamer;
import io.crate.execution.engine.distribution.StreamBucket;

public class JobResponse extends TransportResponse {

    private final List<StreamBucket> directResponse;

    public JobResponse(List<StreamBucket> directResponse) {
        this.directResponse = directResponse;
    }

    public List<StreamBucket> getDirectResponses(Streamer<?>[] streamers) {
        for (StreamBucket bucket : directResponse) {
            bucket.streamers(streamers);
        }
        return directResponse;
    }

    public boolean hasDirectResponses() {
        return !directResponse.isEmpty();
    }

    public JobResponse(StreamInput in) throws IOException {
        int size = in.readVInt();
        directResponse = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            StreamBucket bucket = new StreamBucket(in);
            directResponse.add(bucket);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(directResponse.size());
        for (StreamBucket bucket : directResponse) {
            bucket.writeTo(out);
        }
    }
}

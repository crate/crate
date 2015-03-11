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

package io.crate.executor.transport;

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class NodeCollectResponse extends TransportResponse {

    private Bucket rows;
    private final Streamer<?>[] streamers;

    public NodeCollectResponse(Streamer<?>[] streamers) {
        this.streamers = streamers;
    }

    public void rows(Bucket rows) {
        this.rows = rows;
    }

    public Bucket rows() {
        return rows;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        StreamBucket bucket = new StreamBucket(streamers);
        bucket.readFrom(in);
        rows = bucket;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        StreamBucket.writeBucket(out, streamers, rows);
    }
}

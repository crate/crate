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

package io.crate.executor.transport.merge;

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.StreamBucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public class NodeMergeResponse extends TransportResponse {

    private final Streamer<?>[] streamers;
    private Bucket rows;


    public NodeMergeResponse(Streamer<?>[] streamers, Bucket rows) {
        this.streamers = streamers;
        this.rows = rows;
    }

    public NodeMergeResponse(Streamer<?>[] streamers) {
        this.streamers = streamers;
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
        if (rows instanceof Streamable){
            ((Streamable) rows).writeTo(out);
        }
        StreamBucket.StreamWriter writer = new StreamBucket.StreamWriter(out, streamers, rows.size());
        writer.addAll(rows);
    }
}

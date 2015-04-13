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

import io.crate.Streamer;
import io.crate.core.collections.Bucket;
import io.crate.exceptions.UnknownUpstreamFailure;
import io.crate.executor.transport.StreamBucket;
import org.elasticsearch.common.io.ThrowableObjectInputStream;
import org.elasticsearch.common.io.ThrowableObjectOutputStream;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;

public class DistributedResultRequest extends TransportRequest {

    private int executionNodeId;
    private int bucketIdx;

    private Streamer<?>[] streamers;
    private Bucket rows;
    private UUID jobId;

    private Throwable throwable = null;

    public DistributedResultRequest() {
    }

    public DistributedResultRequest(UUID jobId, int executionNodeId, int bucketIdx, Streamer<?>[] streamers) {
        this.jobId = jobId;
        this.executionNodeId = executionNodeId;
        this.bucketIdx = bucketIdx;
        this.streamers = streamers;
    }

    public UUID jobId() {
        return jobId;
    }

    public int executionNodeId() {
        return executionNodeId;
    }

    public int bucketIdx() {
        return bucketIdx;
    }

    public void streamers(Streamer<?>[] streamers) {
        if (rows instanceof StreamBucket) {
            assert streamers != null;
            ((StreamBucket) rows).streamers(streamers);
        }
        this.streamers = streamers;
    }

    public boolean rowsCanBeRead(){
        if (rows instanceof StreamBucket){
            return streamers != null;
        }
        return true;
    }

    public Bucket rows() {
        return rows;
    }

    public void rows(Bucket rows) {
        this.rows = rows;
    }

    public boolean isLast() {
        return true; // TODO: make settable
    }

    public void throwable(Throwable throwable) {
        this.throwable = throwable;
    }

    @Nullable
    public Throwable throwable() {
        return throwable;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobId = new UUID(in.readLong(), in.readLong());
        executionNodeId = in.readVInt();
        bucketIdx = in.readVInt();

        boolean failure = in.readBoolean();
        if (failure) {
            ThrowableObjectInputStream tis = new ThrowableObjectInputStream(in);
            try {
                throwable = (Throwable) tis.readObject();
            } catch (ClassNotFoundException e) {
                throwable = new UnknownUpstreamFailure();
            }
        } else {
            StreamBucket bucket = new StreamBucket(streamers);
            bucket.readFrom(in);
            rows = bucket;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(executionNodeId);
        out.writeVInt(bucketIdx);

        boolean failure = throwable != null;
        out.writeBoolean(failure);
        if (failure) {
            ThrowableObjectOutputStream too = new ThrowableObjectOutputStream(out);
            too.writeObject(throwable);
        } else {
            // TODO: we should not rely on another bucket in this class and instead write to the stream directly
            StreamBucket.writeBucket(out, streamers, rows);
        }
    }
}

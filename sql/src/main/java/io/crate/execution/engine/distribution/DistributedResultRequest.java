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

package io.crate.execution.engine.distribution;

import io.crate.Streamer;
import io.crate.data.Bucket;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.UUID;

public class DistributedResultRequest extends TransportRequest {

    private byte inputId;
    private int executionPhaseId;
    private int bucketIdx;

    private Streamer<?>[] streamers;
    private Bucket rows;
    private UUID jobId;
    private boolean isLast = true;

    private Throwable throwable = null;
    private boolean isKilled = false;

    public DistributedResultRequest() {
    }

    private DistributedResultRequest(UUID jobId, byte inputId, int executionPhaseId, int bucketIdx) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.bucketIdx = bucketIdx;
        this.inputId = inputId;
    }

    public DistributedResultRequest(UUID jobId,
                                    int executionPhaseId,
                                    byte inputId,
                                    int bucketIdx,
                                    Streamer<?>[] streamers,
                                    Bucket rows,
                                    boolean isLast) {
        this(jobId, inputId, executionPhaseId, bucketIdx);
        this.streamers = streamers;
        this.rows = rows;
        this.isLast = isLast;
    }

    public DistributedResultRequest(UUID jobId,
                                    int executionPhaseId,
                                    byte inputId,
                                    int bucketIdx,
                                    Throwable throwable,
                                    boolean isKilled) {
        this(jobId, inputId, executionPhaseId, bucketIdx);
        this.throwable = throwable;
        this.isKilled = isKilled;
    }

    public UUID jobId() {
        return jobId;
    }

    public int executionPhaseId() {
        return executionPhaseId;
    }

    public byte executionPhaseInputId() {
        return inputId;
    }

    public int bucketIdx() {
        return bucketIdx;
    }

    public void streamers(Streamer<?>[] streamers) {
        if (rows instanceof StreamBucket) {
            assert streamers != null : "streamers must not be null";
            ((StreamBucket) rows).streamers(streamers);
        }
        this.streamers = streamers;
    }

    public boolean rowsCanBeRead() {
        if (rows instanceof StreamBucket) {
            return streamers != null;
        }
        return true;
    }

    public Bucket rows() {
        return rows;
    }

    public boolean isLast() {
        return isLast;
    }

    @Nullable
    public Throwable throwable() {
        return throwable;
    }

    public boolean isKilled() {
        return isKilled;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobId = new UUID(in.readLong(), in.readLong());
        executionPhaseId = in.readVInt();
        bucketIdx = in.readVInt();
        isLast = in.readBoolean();
        inputId = in.readByte();

        boolean failure = in.readBoolean();
        if (failure) {
            throwable = in.readException();
            isKilled = in.readBoolean();
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
        out.writeVInt(executionPhaseId);
        out.writeVInt(bucketIdx);
        out.writeBoolean(isLast);
        out.writeByte(inputId);

        boolean failure = throwable != null;
        out.writeBoolean(failure);
        if (failure) {
            out.writeException(throwable);
            out.writeBoolean(isKilled);
        } else {
            // TODO: we should not rely on another bucket in this class and instead write to the stream directly
            StreamBucket.writeBucket(out, streamers, rows);
        }
    }
}

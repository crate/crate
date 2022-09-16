/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import java.io.IOException;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.StoreFileMetadata;

public final class RecoveryFileChunkRequest extends RecoveryTransportRequest {
    private final boolean lastChunk;
    private final long recoveryId;
    private final ShardId shardId;
    private final long position;
    private final BytesReference content;
    private final StoreFileMetadata metadata;
    private final long sourceThrottleTimeInNanos;

    private final int totalTranslogOps;

    public RecoveryFileChunkRequest(long recoveryId,
                                    final long requestSeqNo,
                                    ShardId shardId,
                                    StoreFileMetadata metadata,
                                    long position,
                                    BytesReference content,
                                    boolean lastChunk,
                                    int totalTranslogOps,
                                    long sourceThrottleTimeInNanos) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.metadata = metadata;
        this.position = position;
        this.content = content;
        this.lastChunk = lastChunk;
        this.totalTranslogOps = totalTranslogOps;
        this.sourceThrottleTimeInNanos = sourceThrottleTimeInNanos;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public String name() {
        return metadata.name();
    }

    public long position() {
        return position;
    }

    public long length() {
        return metadata.length();
    }

    public BytesReference content() {
        return content;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long sourceThrottleTimeInNanos() {
        return sourceThrottleTimeInNanos;
    }

    public RecoveryFileChunkRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        final String name = in.readString();
        position = in.readVLong();
        long length = in.readVLong();
        String checksum = in.readString();
        content = in.readBytesReference();
        Version writtenBy = Lucene.parseVersionLenient(in.readString(), null);
        assert writtenBy != null;
        metadata = new StoreFileMetadata(name, length, checksum, writtenBy);
        lastChunk = in.readBoolean();
        totalTranslogOps = in.readVInt();
        sourceThrottleTimeInNanos = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeString(metadata.name());
        out.writeVLong(position);
        out.writeVLong(metadata.length());
        out.writeString(metadata.checksum());
        out.writeBytesReference(content);
        out.writeString(metadata.writtenBy().toString());
        out.writeBoolean(lastChunk);
        out.writeVInt(totalTranslogOps);
        out.writeLong(sourceThrottleTimeInNanos);
    }

    @Override
    public String toString() {
        return shardId + ": name='" + name() + '\'' +
                ", position=" + position +
                ", length=" + length();
    }

    public StoreFileMetadata metadata() {
        return metadata;
    }

    /**
     * Returns <code>true</code> if this chunk is the last chunk in the stream.
     */
    public boolean lastChunk() {
        return lastChunk;
    }
}

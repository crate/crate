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

package io.crate.blob;

import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.UUID;

public class PutChunkReplicaRequest extends ReplicationRequest<PutChunkReplicaRequest> implements IPutChunkRequest {

    public final String sourceNodeId;
    public final UUID transferId;
    public final long currentPos;
    public final BytesReference content;
    public final boolean isLast;

    public PutChunkReplicaRequest(ShardId shardId,
                                  String sourceNodeId,
                                  UUID transferId,
                                  long currentPos,
                                  BytesReference content,
                                  boolean isLast) {
        super(shardId);
        this.sourceNodeId = sourceNodeId;
        this.transferId = transferId;
        this.currentPos = currentPos;
        this.content = content;
        this.isLast = isLast;
    }

    public PutChunkReplicaRequest(StreamInput in) throws IOException {
        super(in);
        sourceNodeId = in.readString();
        transferId = new UUID(in.readLong(), in.readLong());
        currentPos = in.readVInt();
        content = in.readBytesReference();
        isLast = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceNodeId);
        out.writeLong(transferId.getMostSignificantBits());
        out.writeLong(transferId.getLeastSignificantBits());
        out.writeVLong(currentPos);
        out.writeBytesReference(content);
        out.writeBoolean(isLast);
    }

    public BytesReference content() {
        return content;
    }

    public UUID transferId() {
        return transferId;
    }

    public boolean isLast() {
        return isLast;
    }

    @Override
    public String toString() {
        return "PutChunkReplicaRequest{" +
               "sourceNodeId='" + sourceNodeId + '\'' +
               ", transferId=" + transferId +
               ", currentPos=" + currentPos +
               ", isLast=" + isLast +
               '}';
    }
}

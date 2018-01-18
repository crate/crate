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

package io.crate.blob;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.UUID;

public class PutChunkReplicaRequest extends ReplicationRequest<PutChunkReplicaRequest> implements IPutChunkRequest {


    public String sourceNodeId;
    public UUID transferId;
    public long currentPos;
    public BytesReference content;
    public boolean isLast;

    public PutChunkReplicaRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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

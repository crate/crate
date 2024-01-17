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

import java.io.IOException;
import java.util.UUID;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import io.crate.common.Hex;

public class PutChunkRequest extends BlobTransferRequest<PutChunkRequest> {

    private byte[] digest;
    private long currentPos;

    public PutChunkRequest(ShardId shardId, byte[] digest, UUID transferId,
                           BytesReference content, long currentPos, boolean last) {
        super(shardId, transferId, content, last);
        this.digest = digest;
        this.currentPos = currentPos;
    }

    public PutChunkRequest() {
        super();
    }

    public String digest() {
        return Hex.encodeHexString(digest);
    }

    public long currentPos() {
        return currentPos;
    }

    public PutChunkRequest(StreamInput in) throws IOException {
        super(in);
        digest = new byte[20];
        in.read(digest);
        currentPos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.write(digest);
        out.writeVLong(currentPos);
    }
}

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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class BlobStartPrefixSyncRequest extends BlobRecoveryRequest {

    private byte prefix;
    private ShardId shardId;

    public BlobStartPrefixSyncRequest() {
    }

    public BlobStartPrefixSyncRequest(long recoveryId, ShardId shardId, byte prefix) {
        super(recoveryId);
        this.prefix = prefix;
        this.shardId = shardId;
    }

    public byte prefix() {
        return prefix;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        prefix = in.readByte();
        shardId = ShardId.readShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(prefix);
        shardId.writeTo(out);
    }

}



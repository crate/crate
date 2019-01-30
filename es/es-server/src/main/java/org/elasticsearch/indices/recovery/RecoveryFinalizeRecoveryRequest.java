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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class RecoveryFinalizeRecoveryRequest extends TransportRequest {

    private long recoveryId;
    private ShardId shardId;
    private long globalCheckpoint;

    public RecoveryFinalizeRecoveryRequest() {
    }

    RecoveryFinalizeRecoveryRequest(final long recoveryId, final ShardId shardId, final long globalCheckpoint) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.globalCheckpoint = globalCheckpoint;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public long globalCheckpoint() {
        return globalCheckpoint;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        recoveryId = in.readLong();
        shardId = ShardId.readShardId(in);
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            globalCheckpoint = in.readZLong();
        } else {
            globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            out.writeZLong(globalCheckpoint);
        }
    }

}

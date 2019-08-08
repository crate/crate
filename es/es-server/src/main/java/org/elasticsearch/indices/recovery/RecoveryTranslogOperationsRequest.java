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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;

public class RecoveryTranslogOperationsRequest extends TransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
    private final List<Translog.Operation> operations;
    private final int totalTranslogOps;
    private final long maxSeenAutoIdTimestampOnPrimary;
    private final long maxSeqNoOfUpdatesOrDeletesOnPrimary;

    RecoveryTranslogOperationsRequest(long recoveryId,
                                      ShardId shardId,
                                      List<Translog.Operation> operations,
                                      int totalTranslogOps,
                                      long maxSeenAutoIdTimestampOnPrimary,
                                      long maxSeqNoOfUpdatesOrDeletesOnPrimary) {
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.operations = operations;
        this.totalTranslogOps = totalTranslogOps;
        this.maxSeenAutoIdTimestampOnPrimary = maxSeenAutoIdTimestampOnPrimary;
        this.maxSeqNoOfUpdatesOrDeletesOnPrimary = maxSeqNoOfUpdatesOrDeletesOnPrimary;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public List<Translog.Operation> operations() {
        return operations;
    }

    public int totalTranslogOps() {
        return totalTranslogOps;
    }

    public long maxSeenAutoIdTimestampOnPrimary() {
        return maxSeenAutoIdTimestampOnPrimary;
    }

    public long maxSeqNoOfUpdatesOrDeletesOnPrimary() {
        return maxSeqNoOfUpdatesOrDeletesOnPrimary;
    }

    public RecoveryTranslogOperationsRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        operations = Translog.readOperations(in, "recovery");
        totalTranslogOps = in.readVInt();
        maxSeenAutoIdTimestampOnPrimary = in.readZLong();
        maxSeqNoOfUpdatesOrDeletesOnPrimary = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        Translog.writeOperations(out, operations);
        out.writeVInt(totalTranslogOps);
        out.writeZLong(maxSeenAutoIdTimestampOnPrimary);
        out.writeZLong(maxSeqNoOfUpdatesOrDeletesOnPrimary);
    }
}

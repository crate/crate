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

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.indices.flush.ShardsSyncedFlushResult;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

/**
 * The result of performing a sync flush operation on all shards of multiple indices
 */
public class SyncedFlushResponse extends TransportResponse {

    private final Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex;
    private final ShardCounts shardCounts;

    public SyncedFlushResponse(Map<String, List<ShardsSyncedFlushResult>> shardsResultPerIndex) {
        // shardsResultPerIndex is never modified after it is passed to this
        // constructor so this is safe even though shardsResultPerIndex is a
        // ConcurrentHashMap
        this.shardsResultPerIndex = unmodifiableMap(shardsResultPerIndex);
        this.shardCounts = calculateShardCounts(Iterables.flatten(shardsResultPerIndex.values()));
    }

    /**
     * total number shards, including replicas, both assigned and unassigned
     */
    public int totalShards() {
        return shardCounts.total;
    }

    /**
     * total number of shards for which the operation failed
     */
    public int failedShards() {
        return shardCounts.failed;
    }

    /**
     * total number of shards which were successfully sync-flushed
     */
    public int successfulShards() {
        return shardCounts.successful;
    }


    private static ShardCounts calculateShardCounts(Iterable<ShardsSyncedFlushResult> results) {
        int total = 0, successful = 0, failed = 0;
        for (ShardsSyncedFlushResult result : results) {
            total += result.totalShards();
            successful += result.successfulShards();
            if (result.failed()) {
                // treat all shard copies as failed
                failed += result.totalShards();
            } else {
                // some shards may have failed during the sync phase
                failed += result.failedShards().size();
            }
        }
        return new ShardCounts(total, successful, failed);
    }

    static final class ShardCounts implements Writeable {

        public final int total;
        public final int successful;
        public final int failed;

        ShardCounts(int total, int successful, int failed) {
            this.total = total;
            this.successful = successful;
            this.failed = failed;
        }

        public ShardCounts(StreamInput in) throws IOException {
            total = in.readInt();
            successful = in.readInt();
            failed = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(total);
            out.writeInt(successful);
            out.writeInt(failed);
        }
    }

    public SyncedFlushResponse(StreamInput in) throws IOException {
        shardCounts = new ShardCounts(in);
        Map<String, List<ShardsSyncedFlushResult>> tmpShardsResultPerIndex = new HashMap<>();
        int numShardsResults = in.readInt();
        for (int i = 0 ; i < numShardsResults; i++) {
            String index = in.readString();
            List<ShardsSyncedFlushResult> shardsSyncedFlushResults = new ArrayList<>();
            int numShards = in.readInt();
            for (int j = 0; j < numShards; j++) {
                shardsSyncedFlushResults.add(new ShardsSyncedFlushResult(in));
            }
            tmpShardsResultPerIndex.put(index, shardsSyncedFlushResults);
        }
        shardsResultPerIndex = Collections.unmodifiableMap(tmpShardsResultPerIndex);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardCounts.writeTo(out);
        out.writeInt(shardsResultPerIndex.size());
        for (Map.Entry<String, List<ShardsSyncedFlushResult>> entry : shardsResultPerIndex.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (ShardsSyncedFlushResult shardsSyncedFlushResult : entry.getValue()) {
                shardsSyncedFlushResult.writeTo(out);
            }
        }
    }
}

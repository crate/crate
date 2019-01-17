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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;

public class ShardStats implements Streamable, Writeable {
    private ShardRouting shardRouting;
    private CommonStats commonStats;
    @Nullable
    private CommitStats commitStats;
    @Nullable
    private SeqNoStats seqNoStats;
    private String dataPath;
    private String statePath;
    private boolean isCustomDataPath;

    ShardStats() {
    }

    public ShardStats(ShardRouting routing, ShardPath shardPath, CommonStats commonStats, CommitStats commitStats, SeqNoStats seqNoStats) {
        this.shardRouting = routing;
        this.dataPath = shardPath.getRootDataPath().toString();
        this.statePath = shardPath.getRootStatePath().toString();
        this.isCustomDataPath = shardPath.isCustomDataPath();
        this.commitStats = commitStats;
        this.commonStats = commonStats;
        this.seqNoStats = seqNoStats;
    }

    /**
     * The shard routing information (cluster wide shard state).
     */
    public ShardRouting getShardRouting() {
        return this.shardRouting;
    }

    public CommonStats getStats() {
        return this.commonStats;
    }

    @Nullable
    public CommitStats getCommitStats() {
        return this.commitStats;
    }

    @Nullable
    public SeqNoStats getSeqNoStats() {
        return this.seqNoStats;
    }

    public String getDataPath() {
        return dataPath;
    }

    public String getStatePath() {
        return statePath;
    }

    public boolean isCustomDataPath() {
        return isCustomDataPath;
    }

    public static ShardStats readShardStats(StreamInput in) throws IOException {
        ShardStats stats = new ShardStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        commonStats = new CommonStats(in);
        commitStats = CommitStats.readOptionalCommitStatsFrom(in);
        statePath = in.readString();
        dataPath = in.readString();
        isCustomDataPath = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            seqNoStats = in.readOptionalWriteable(SeqNoStats::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        commonStats.writeTo(out);
        out.writeOptionalStreamable(commitStats);
        out.writeString(statePath);
        out.writeString(dataPath);
        out.writeBoolean(isCustomDataPath);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            out.writeOptionalWriteable(seqNoStats);
        }
    }
}

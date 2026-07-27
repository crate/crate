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

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardPath;

public class ShardStats implements Writeable {

    private final ShardRouting shardRouting;
    private final CommonStats commonStats;
    private final String dataPath;

    public ShardStats(
            ShardRouting routing,
            ShardPath shardPath,
            CommonStats commonStats) {
        this.shardRouting = routing;
        this.dataPath = shardPath.getRootDataPath().toString();
        this.commonStats = commonStats;
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

    public String getDataPath() {
        return dataPath;
    }

    public ShardStats(StreamInput in) throws IOException {
        shardRouting = new ShardRouting(in);
        commonStats = new CommonStats(in);
        boolean before650 = in.getVersion().before(Version.V_6_5_0);
        if (before650) {
            in.readOptionalWriteable(CommitStats::new);
            in.readString(); // statePath
        }
        dataPath = in.readString();
        if (before650) {
            in.readBoolean(); // isCustomDataPath
            in.readOptionalWriteable(SeqNoStats::new);
            in.readOptionalWriteable(RetentionLeaseStats::new);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardRouting.writeTo(out);
        commonStats.writeTo(out);
        boolean before650 = out.getVersion().before(Version.V_6_5_0);
        if (before650) {
            out.writeOptionalWriteable(null); // commitStats
            // Was statePath, use dataPath instead as dummy - it was unused anyway
            out.writeString(dataPath);
        }
        out.writeString(dataPath);
        if (before650) {
            out.writeBoolean(false); // isCustomDataPath - was unused, use false dummy value
            out.writeOptionalWriteable(null); // seqNoStats
            out.writeOptionalWriteable(null); // retentionLeaseStats
        }
    }
}

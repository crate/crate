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

package org.elasticsearch.action.admin.indices.upgrade.get;

import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.Iterator;

public class IndexShardUpgradeStatus implements Iterable<ShardUpgradeStatus> {

    private final ShardId shardId;

    private final ShardUpgradeStatus[] shards;

    IndexShardUpgradeStatus(ShardId shardId, ShardUpgradeStatus[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public ShardUpgradeStatus getAt(int i) {
        return shards[i];
    }

    public ShardUpgradeStatus[] getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ShardUpgradeStatus> iterator() {
        return Arrays.stream(shards).iterator();
    }

    public long getTotalBytes() {
        long totalBytes = 0;
        for (ShardUpgradeStatus indexShardUpgradeStatus : shards) {
            totalBytes += indexShardUpgradeStatus.getTotalBytes();
        }
        return totalBytes;
    }

    public long getToUpgradeBytes() {
        long upgradeBytes = 0;
        for (ShardUpgradeStatus indexShardUpgradeStatus : shards) {
            upgradeBytes += indexShardUpgradeStatus.getToUpgradeBytes();
        }
        return upgradeBytes;
    }

    public long getToUpgradeBytesAncient() {
        long upgradeBytesAncient = 0;
        for (ShardUpgradeStatus indexShardUpgradeStatus : shards) {
            upgradeBytesAncient += indexShardUpgradeStatus.getToUpgradeBytesAncient();
        }
        return upgradeBytesAncient;
    }
}
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

package io.crate.metadata.shard.unassigned;

import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;

/**
 * This class represents an unassigned shard
 * <p>
 * An UnassignedShard is a shard that is not yet assigned to any node and therefore doesn't really exist anywhere.
 * <p>
 * The {@link io.crate.metadata.sys.SysShardsTableInfo} will encode any shards that aren't assigned to a node
 * by negating them using {@link #markUnassigned(int)}
 * <p>
 * The {@link io.crate.execution.engine.collect.sources.ShardCollectSource} will then collect UnassignedShard
 * instances for all shardIds that are negative.
 * <p>
 * This is only for "select ... from sys.shards" queries.
 */
public final class UnassignedShard {

    public static boolean isUnassigned(int shardId) {
        return shardId < 0;
    }

    /**
     * valid shard ids are vom 0 to int.max
     * this method negates a shard id (0 becomes -1, 1 becomes -2, etc.)
     * <p>
     * if the given id is already negative it is returned as is
     */
    public static int markUnassigned(int id) {
        if (id >= 0) {
            return (id + 1) * -1;
        }
        return id;
    }

    /**
     * converts negative shard ids back to positive
     * (-1 becomes 0, -2 becomes 1 - the opposite of {@link #markUnassigned(int)}
     */
    public static int markAssigned(int shard) {
        if (shard < 0) {
            return (shard * -1) - 1;
        }
        return shard;
    }

    private final String schemaName;
    private final String tableName;
    private final Boolean primary;
    private final int id;
    private final String partitionIdent;
    private final String state;
    private final boolean orphanedPartition;

    private static final String UNASSIGNED = "UNASSIGNED";
    private static final String INITIALIZING = "INITIALIZING";


    public UnassignedShard(int shardId,
                           String indexName,
                           ClusterService clusterService,
                           Boolean primary,
                           ShardRoutingState state) {
        IndexParts indexParts = IndexName.decode(indexName);
        this.schemaName = indexParts.schema();
        this.tableName = indexParts.table();
        this.partitionIdent = indexParts.partitionIdent();
        this.orphanedPartition = indexParts.isPartitioned()
                                 && !clusterService.state().metadata().hasConcreteIndex(tableName);
        this.primary = primary;
        this.id = shardId;
        this.state = state == ShardRoutingState.UNASSIGNED ? UNASSIGNED : INITIALIZING;
    }

    public String tableName() {
        return tableName;
    }

    public int id() {
        return id;
    }

    public String schemaName() {
        return schemaName;
    }

    public String partitionIdent() {
        return partitionIdent;
    }

    public Boolean primary() {
        return primary;
    }

    public String state() {
        return state;
    }

    public Boolean orphanedPartition() {
        return orphanedPartition;
    }
}

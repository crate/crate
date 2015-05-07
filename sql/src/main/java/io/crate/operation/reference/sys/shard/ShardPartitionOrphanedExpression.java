/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
package io.crate.operation.reference.sys.shard;

import io.crate.metadata.PartitionName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;

public class ShardPartitionOrphanedExpression extends SysShardExpression<Boolean> {

    public static final String NAME = "orphan_partition";
    private final ClusterService clusterService;
    private String tableName;
    private boolean isPartition = false;


    @Inject
    public ShardPartitionOrphanedExpression(ShardId shardId, ClusterService clusterService) {
        this.clusterService = clusterService;
        tableName = shardId.getIndex();
        try {
            tableName = PartitionName.tableName(tableName);
            isPartition = true;
        } catch (IllegalArgumentException e) {
            // no partition
        }
    }

    @Override
    public Boolean value() {
        if (!isPartition) {
            return false;
        }
        if (!clusterService.state().metaData().hasConcreteIndex(tableName)) {
            return true;
        }
        return false;
    }

}

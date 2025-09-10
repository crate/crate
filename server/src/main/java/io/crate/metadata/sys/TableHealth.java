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

package io.crate.metadata.sys;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

record TableHealth(RelationName relationName,
                   @Nullable String partitionIdent,
                   Health health,
                   long missingShards,
                   long underreplicatedShards) {

    enum Health {
        GREEN,
        YELLOW,
        RED;

        public short severity() {
            return (short) (ordinal() + 1);
        }
    }

    public static Iterable<TableHealth> compute(ClusterState clusterState) {
        Metadata metadata = clusterState.metadata();
        RoutingTable routingTable = clusterState.routingTable();
        List<RelationMetadata> relations = metadata.relations(RelationMetadata.class);
        ArrayList<TableHealth> result = new ArrayList<>();
        for (var relation : relations) {
            for (String indexUUID : relation.indexUUIDs()) {
                IndexMetadata indexMetadata = metadata.index(indexUUID);
                IndexRoutingTable indexRoutingTable = routingTable.index(indexUUID);
                if (indexMetadata == null || indexRoutingTable == null) {
                    continue;
                }

                // Distinguish between new shards and shards that have been assigned before
                // to decide health:
                // Missing primaries for a new partition/table is okay (yellow)
                // Missing primaries for shards that were assigned already are bad (red)
                int missingNewPrimaryShards = 0;
                int missingUsedPrimaryShards = 0;
                int underreplicatedShards = 0;
                for (var indexShardRoutingTable : indexRoutingTable) {
                    for (var shardRouting : indexShardRoutingTable) {
                        if (shardRouting.active()) {
                            continue;
                        }
                        if (shardRouting.primary()) {
                            var healthStatus = ClusterShardHealth.getInactivePrimaryHealth(shardRouting);
                            if (healthStatus == ClusterHealthStatus.YELLOW) {
                                missingNewPrimaryShards += 1;
                            } else {
                                missingUsedPrimaryShards += 1;
                            }
                        } else {
                            underreplicatedShards += 1;
                        }
                    }
                }
                Health health = missingUsedPrimaryShards > 0
                    ? Health.RED
                    : (underreplicatedShards + missingNewPrimaryShards) > 0 ? Health.YELLOW : Health.GREEN;
                result.add(new TableHealth(
                    relation.name(),
                    PartitionName.encodeIdent(indexMetadata.partitionValues()),
                    health,
                    missingNewPrimaryShards + missingUsedPrimaryShards,
                    underreplicatedShards
                ));
            }
        }
        return result;
    }

    public String fqn() {
        return relationName.fqn();
    }

    public String healthText() {
        return health.toString();
    }

    public short severity() {
        return health.severity();
    }

    @Override
    public String toString() {
        return "TableHealth{" +
               "name='" + relationName + '\'' +
               ", partitionIdent='" + partitionIdent + '\'' +
               ", health=" + health +
               ", missingShards=" + missingShards +
               ", underreplicatedShards=" + underreplicatedShards +
               '}';
    }
}

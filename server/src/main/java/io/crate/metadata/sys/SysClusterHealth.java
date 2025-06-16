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

import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.SHORT;
import static io.crate.types.DataTypes.STRING;

import java.util.List;
import java.util.Set;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.rest.RestStatus;

import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;

public class SysClusterHealth {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "cluster_health");

    static SystemTable<ClusterHealth> INSTANCE = SystemTable.<ClusterHealth>builder(IDENT)
        .add("health", STRING, ClusterHealth::getHealth)
        .add("severity", SHORT, ClusterHealth::getSeverity)
        .add("description", STRING, ClusterHealth::description)
        .add("missing_shards", LONG, ClusterHealth::missingShards)
        .add("underreplicated_shards", LONG, ClusterHealth::underreplicatedShards)
        .add("pending_tasks", LONG, ClusterHealth::pendingTasks)
        .withRouting((state, ignored, ignored2) -> Routing.forMasterNode(IDENT, state))
        .build();

    public static Iterable<ClusterHealth> compute(ClusterState clusterState, long numPendingTasks) {
        // Following implementation of {@link org.elasticsearch.cluster.health.ClusterStateHealth}
        Set<ClusterBlock> blocksRed = clusterState.blocks().global(RestStatus.SERVICE_UNAVAILABLE);
        if (!blocksRed.isEmpty()) {
            var block = blocksRed.iterator().next();
            ClusterHealth clusterHealth = new ClusterHealth(
                TableHealth.Health.RED,
                block.description(),
                -1,
                -1,
                numPendingTasks
            );
            return List.of(clusterHealth);
        }
        Set<ClusterBlock> blocksYellow = clusterState.blocks().global(ClusterBlockLevel.METADATA_WRITE);
        final TableHealth.Health clusterHealth = !blocksYellow.isEmpty() ? TableHealth.Health.YELLOW : TableHealth.Health.GREEN;
        final String description = !blocksYellow.isEmpty() ? blocksYellow.iterator().next().description() : "";
        long missingShards = 0;
        long underreplicatedShards = 0;
        TableHealth.Health health = clusterHealth;
        String finalDescription = description;
        for (var tableHealth : TableHealth.compute(clusterState)) {
            if (tableHealth.health().severity() > health.severity()) {
                health = tableHealth.health();
                // shard level health is only set to RED if there are missing primary shards that were
                // allocated before see {@link ClusterShardHealth#getInactivePrimaryHealth()}.
                // Otherwise, the table health is set to YELLOW.
                if (health == TableHealth.Health.RED || tableHealth.getMissingShards() > 0) {
                    finalDescription = "One or more tables are missing shards";
                } else if (health == TableHealth.Health.YELLOW) {
                    finalDescription = "One or more tables have underreplicated shards";
                }
            }
            missingShards += tableHealth.getMissingShards();
            underreplicatedShards += tableHealth.getUnderreplicatedShards();
        }
        return List.of(new ClusterHealth(health, finalDescription, missingShards, underreplicatedShards, numPendingTasks));
    }

    public record ClusterHealth(TableHealth.Health health,
                                String description,
                                long missingShards,
                                long underreplicatedShards,
                                long pendingTasks) {

        public String getHealth() {
            return health.name();
        }

        public short getSeverity() {
            return health.severity();
        }
    }
}

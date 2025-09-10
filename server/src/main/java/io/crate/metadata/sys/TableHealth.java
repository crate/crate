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

import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.IndexName;
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
        var clusterHealth = new ClusterStateHealth(clusterState);
        return StreamSupport.stream(clusterHealth.spliterator(), false)
            .filter(i -> IndexName.isDangling(i.getIndex()) == false)
            .map(TableHealth::map)::iterator;
    }

    private static TableHealth map(ClusterIndexHealth indexHealth) {
        var indexParts = IndexName.decode(indexHealth.getIndex());
        String partitionIdent = null;
        if (indexParts.isPartitioned()) {
            partitionIdent = indexParts.partitionIdent();
        }

        int missingPrimaryShards = Math.max(
            0,
            indexHealth.getNumberOfShards() - indexHealth.getActivePrimaryShards()
        );
        int underreplicatedShards = Math.max(
            0,
            indexHealth.getUnassignedShards() + indexHealth.getInitializingShards() - missingPrimaryShards
        );

        return new TableHealth(
            indexParts.toRelationName(),
            partitionIdent,
            TableHealth.Health.valueOf(indexHealth.getStatus().name()),
            missingPrimaryShards,
            underreplicatedShards
        );
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

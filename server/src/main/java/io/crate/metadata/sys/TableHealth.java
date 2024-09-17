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

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.IndexName;

class TableHealth {

    enum Health {
        GREEN,
        YELLOW,
        RED;

        public short severity() {
            return (short) (ordinal() + 1);
        }
    }

    public static CompletableFuture<Iterable<TableHealth>> compute(ClusterState clusterState) {
        var clusterHealth = new ClusterStateHealth(clusterState);
        return completedFuture(
            StreamSupport.stream(clusterHealth.spliterator(), false)
                .filter(i -> IndexName.isDangling(i.getIndex()) == false)
                .map(TableHealth::map)::iterator
        );
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
            indexParts.table(),
            indexParts.schema(),
            partitionIdent,
            TableHealth.Health.valueOf(indexHealth.getStatus().name()),
            missingPrimaryShards,
            underreplicatedShards
        );
    }


    private final String tableName;
    private final String tableSchema;
    @Nullable
    private final String partitionIdent;
    private final Health health;
    private final long missingShards;
    private final long underreplicatedShards;
    private final String fqn;

    TableHealth(String tableName,
                String tableSchema,
                @Nullable String partitionIdent,
                Health health,
                long missingShards,
                long underreplicatedShards) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.partitionIdent = partitionIdent;
        this.health = health;
        this.missingShards = missingShards;
        this.underreplicatedShards = underreplicatedShards;
        fqn = IndexName.encode(tableSchema, tableName, null);
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    @Nullable
    public String getPartitionIdent() {
        return partitionIdent;
    }

    public String getHealth() {
        return health.toString();
    }

    public short getSeverity() {
        return health.severity();
    }

    public long getMissingShards() {
        return missingShards;
    }

    public long getUnderreplicatedShards() {
        return underreplicatedShards;
    }

    public String fqn() {
        return fqn;
    }

    @Override
    public String toString() {
        return "TableHealth{" +
               "name='" + tableName + '\'' +
               ", schema='" + tableSchema + '\'' +
               ", partitionIdent='" + partitionIdent + '\'' +
               ", health=" + health +
               ", missingShards=" + missingShards +
               ", underreplicatedShards=" + underreplicatedShards +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TableHealth that = (TableHealth) o;

        if (missingShards != that.missingShards) return false;
        if (underreplicatedShards != that.underreplicatedShards) return false;
        if (!tableName.equals(that.tableName)) return false;
        if (!tableSchema.equals(that.tableSchema)) return false;
        if (partitionIdent != null ? !partitionIdent.equals(that.partitionIdent) : that.partitionIdent != null)
            return false;
        return health == that.health;
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + tableSchema.hashCode();
        result = 31 * result + (partitionIdent != null ? partitionIdent.hashCode() : 0);
        result = 31 * result + health.hashCode();
        result = 31 * result + (int) (missingShards ^ (missingShards >>> 32));
        result = 31 * result + (int) (underreplicatedShards ^ (underreplicatedShards >>> 32));
        return result;
    }
}

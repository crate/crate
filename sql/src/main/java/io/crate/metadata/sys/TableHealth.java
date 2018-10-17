/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.sys;

import io.crate.metadata.IndexParts;

import javax.annotation.Nullable;

class TableHealth {

    enum Health {
        GREEN,
        YELLOW,
        RED;

        public short severity() {
            return (short) (ordinal() + 1);
        }
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
        fqn = IndexParts.toIndexName(tableSchema, tableName, null);
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
}

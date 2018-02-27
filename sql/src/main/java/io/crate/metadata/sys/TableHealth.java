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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

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

    private final BytesRef tableName;
    private final BytesRef tableSchema;
    @Nullable
    private final BytesRef partitionIdent;
    private final Health health;
    private final long missingShards;
    private final long underreplicatedShards;
    private final String fqn;

    TableHealth(BytesRef tableName,
                BytesRef tableSchema,
                @Nullable BytesRef partitionIdent,
                Health health,
                long missingShards,
                long underreplicatedShards) {
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.partitionIdent = partitionIdent;
        this.health = health;
        this.missingShards = missingShards;
        this.underreplicatedShards = underreplicatedShards;
        fqn = IndexParts.toIndexName(BytesRefs.toString(tableSchema), BytesRefs.toString(tableName), null);
    }

    public BytesRef getTableName() {
        return tableName;
    }

    public BytesRef getTableSchema() {
        return tableSchema;
    }

    @Nullable
    public BytesRef getPartitionIdent() {
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

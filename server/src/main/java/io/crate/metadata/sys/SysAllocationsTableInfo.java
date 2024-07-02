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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

import io.crate.expression.reference.sys.shard.SysAllocation;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class SysAllocationsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "allocations");

    public static SystemTable<SysAllocation> INSTANCE = SystemTable.<SysAllocation>builder(IDENT)
        .add("table_schema", STRING, SysAllocation::tableSchema)
        .add("table_name", STRING, SysAllocation::tableName)
        .add("partition_ident", STRING, SysAllocation::partitionIdent)
        .add("shard_id", INTEGER, SysAllocation::shardId)
        .add("node_id", STRING, SysAllocation::nodeId)
        .add("primary", BOOLEAN, SysAllocation::primary)
        .add("current_state", STRING, s -> s.currentState().toString())
        .add("explanation", STRING, SysAllocation::explanation)
        .startObjectArray("decisions", SysAllocation::decisions)
            .add("node_id", STRING, SysAllocation.SysAllocationNodeDecision::nodeId)
            .add("node_name", STRING, SysAllocation.SysAllocationNodeDecision::nodeName)
            .add("explanations", STRING_ARRAY, SysAllocation.SysAllocationNodeDecision::explanations)
        .endObjectArray()
        .setPrimaryKeys(
            ColumnIdent.of("table_schema"),
            ColumnIdent.of("table_name"),
            ColumnIdent.of("partition_ident"),
            ColumnIdent.of("shard_id")
        )
        .build();
}

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

import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.SystemTable;

public class SysHealth {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "health");

    static SystemTable<TableHealth> INSTANCE = SystemTable.<TableHealth>builder(IDENT)
        .add("table_name", STRING, TableHealth::getTableName)
        .add("table_schema", STRING, TableHealth::getTableSchema)
        .add("partition_ident", STRING, TableHealth::getPartitionIdent)
        .add("health", STRING, TableHealth::getHealth)
        .add("severity", SHORT, TableHealth::getSeverity)
        .add("missing_shards", LONG, TableHealth::getMissingShards)
        .add("underreplicated_shards", LONG, TableHealth::getUnderreplicatedShards)
        .withRouting((state, routingProvider, sessionSettings) ->
            Routing.forTableOnSingleNode(IDENT, state.nodes().getMasterNodeId()))
        .build();
}

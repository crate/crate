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

import io.crate.action.sql.SessionContext;
import io.crate.expression.reference.sys.snapshot.SysSnapshot;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.SystemTable;

import org.elasticsearch.cluster.ClusterState;


import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.TIMESTAMPZ;

public class SysSnapshotsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "snapshots");

    static SystemTable<SysSnapshot> create() {
        return SystemTable.<SysSnapshot>builder(IDENT)
            .add("name", STRING, SysSnapshot::name)
            .add("repository", STRING, SysSnapshot::repository)
            .add("concrete_indices", STRING_ARRAY, SysSnapshot::concreteIndices)
            .add("tables", STRING_ARRAY, SysSnapshot::tables)
            .add("started", TIMESTAMPZ, SysSnapshot::started)
            .add("finished", TIMESTAMPZ, SysSnapshot::finished)
            .add("version", STRING, SysSnapshot::version)
            .add("state", STRING, SysSnapshot::state)
            .add("failures", STRING_ARRAY, SysSnapshot::failures)
            .setPrimaryKeys(new ColumnIdent("name"), new ColumnIdent("repository"))
            .withRouting(SysSnapshotsTableInfo::getRouting)
            .build();
    }

    private static Routing getRouting(ClusterState state, RoutingProvider routingProvider, SessionContext sessionContext) {
        // route to random master or data node,
        // because RepositoriesService (and so snapshots info) is only available there
        return routingProvider.forRandomMasterOrDataNode(IDENT, state.getNodes());
    }
}

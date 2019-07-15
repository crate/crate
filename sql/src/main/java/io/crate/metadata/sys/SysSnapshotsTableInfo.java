/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.sys;

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.snapshot.SysSnapshot;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.TIMESTAMPZ;

public class SysSnapshotsTableInfo extends StaticTableInfo<SysSnapshot> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "snapshots");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static class Columns {
        public static final ColumnIdent NAME = new ColumnIdent("name");
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>> expressions() {
        return columnRegistrar().expressions();
    }

    private static ColumnRegistrar<SysSnapshot> columnRegistrar() {
        return new ColumnRegistrar<SysSnapshot>(IDENT, GRANULARITY)
          .register("name", STRING, () -> forFunction(SysSnapshot::name))
          .register("repository", STRING, () -> forFunction(SysSnapshot::repository))
          .register("concrete_indices", STRING_ARRAY, () -> forFunction(SysSnapshot::concreteIndices))
          .register("started", TIMESTAMPZ, () -> forFunction(SysSnapshot::started))
          .register("finished", TIMESTAMPZ, () -> forFunction(SysSnapshot::finished))
          .register("version", STRING, () -> forFunction(SysSnapshot::version))
          .register("state", STRING, () -> forFunction(SysSnapshot::state));
    }

    SysSnapshotsTableInfo() {
        super(IDENT, columnRegistrar(), "name","repository");
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public RelationName ident() {
        return IDENT;
    }

    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        // route to random master or data node,
        // because RepositoriesService (and so snapshots info) is only available there
        return routingProvider.forRandomMasterOrDataNode(IDENT, clusterState.getNodes());
    }
}

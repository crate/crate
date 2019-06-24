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
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterState;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

public class SysSnapshotsTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "snapshots");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static class Columns {
        public static final ColumnIdent NAME = new ColumnIdent("name");
        static final ColumnIdent REPOSITORY = new ColumnIdent("repository");
        static final ColumnIdent CONCRETE_INDICES = new ColumnIdent("concrete_indices");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
        static final ColumnIdent FINISHED = new ColumnIdent("finished");
        public static final ColumnIdent VERSION = new ColumnIdent("version");
        static final ColumnIdent STATE = new ColumnIdent("state");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<SysSnapshot> columnRegistrar() {
        return new ColumnRegistrar<SysSnapshot>(IDENT, GRANULARITY)
          .register("name", DataTypes.STRING, () -> forFunction(SysSnapshot::name))
          .register(Columns.REPOSITORY, DataTypes.STRING, () -> forFunction(SysSnapshot::repository))
          .register(Columns.CONCRETE_INDICES, DataTypes.STRING_ARRAY, () -> forFunction((SysSnapshot s) -> s.concreteIndices().toArray(new String[0])))
          .register(Columns.STARTED, DataTypes.TIMESTAMPZ, () -> forFunction(SysSnapshot::started))
          .register(Columns.FINISHED, DataTypes.TIMESTAMPZ, () -> forFunction(SysSnapshot::finished))
          .register(Columns.VERSION, DataTypes.STRING, () -> forFunction(SysSnapshot::version))
          .register(Columns.STATE, DataTypes.STRING, () -> forFunction(SysSnapshot::state));
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

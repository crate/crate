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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.expression.reference.sys.snapshot.SysSnapshot;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.List;

public class SysSnapshotsTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "snapshots");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.NAME, Columns.REPOSITORY);
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

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysSnapshot>>builder()
            .put(SysSnapshotsTableInfo.Columns.NAME,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::name))
            .put(SysSnapshotsTableInfo.Columns.REPOSITORY,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::repository))
            .put(SysSnapshotsTableInfo.Columns.CONCRETE_INDICES,
                () -> RowContextCollectorExpression.forFunction((SysSnapshot s) -> {
                    List<String> concreteIndices = s.concreteIndices();
                    BytesRef[] indices = new BytesRef[concreteIndices.size()];
                    for (int i = 0; i < concreteIndices.size(); i++) {
                        indices[i] = BytesRefs.toBytesRef(concreteIndices.get(i));
                    }
                    return indices;
                }))
            .put(SysSnapshotsTableInfo.Columns.STARTED,
                () -> RowContextCollectorExpression.forFunction(SysSnapshot::started))
            .put(SysSnapshotsTableInfo.Columns.FINISHED,
                () -> RowContextCollectorExpression.forFunction(SysSnapshot::finished))
            .put(SysSnapshotsTableInfo.Columns.VERSION,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::version))
            .put(SysSnapshotsTableInfo.Columns.STATE,
                () -> RowContextCollectorExpression.objToBytesRef(SysSnapshot::state))
            .build();
    }


    SysSnapshotsTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(Columns.NAME, DataTypes.STRING)
                .register(Columns.REPOSITORY, DataTypes.STRING)
                .register(Columns.CONCRETE_INDICES, new ArrayType(DataTypes.STRING))
                .register(Columns.STARTED, DataTypes.TIMESTAMP)
                .register(Columns.FINISHED, DataTypes.TIMESTAMP)
                .register(Columns.VERSION, DataTypes.STRING)
                .register(Columns.STATE, DataTypes.STRING),
            PRIMARY_KEY);
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

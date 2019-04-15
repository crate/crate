/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.reference.sys.job.JobContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.function.Supplier;

public class SysJobsTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "jobs");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(Columns.ID);

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        static final ColumnIdent USERNAME = new ColumnIdent("username");
        static final ColumnIdent STMT = new ColumnIdent("stmt");
        public static final ColumnIdent STARTED = new ColumnIdent("started");
        static final ColumnIdent NODE = new ColumnIdent("node");
        static final ColumnIdent NODE_ID = new ColumnIdent("node", "id");
        static final ColumnIdent NODE_NAME = new ColumnIdent("node", "name");
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<JobContext>> expressions(Supplier<DiscoveryNode> localNode) {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<JobContext>>builder()
            .put(SysJobsTableInfo.Columns.ID,
                () -> NestableCollectExpression.forFunction(c -> c.id().toString()))
            .put(SysJobsTableInfo.Columns.USERNAME,
                () -> NestableCollectExpression.forFunction(JobContext::username))
            .put(SysJobsTableInfo.Columns.STMT,
                () -> NestableCollectExpression.forFunction(JobContext::stmt))
            .put(SysJobsTableInfo.Columns.STARTED,
                () -> NestableCollectExpression.forFunction(JobContext::started))
            .put(Columns.NODE, () -> NestableCollectExpression.forFunction(ignored -> ImmutableMap.of(
                "id", localNode.get().getId(),
                "name", localNode.get().getName()
            )))
            .put(Columns.NODE_ID, () -> NestableCollectExpression.forFunction(ignored -> localNode.get().getId()))
            .put(Columns.NODE_NAME, () -> NestableCollectExpression.forFunction(ignored -> localNode.get().getName()))
            .build();
    }

    SysJobsTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.DOC)
                .register(Columns.ID, DataTypes.STRING)
                .register(Columns.USERNAME, DataTypes.STRING)
                .register(Columns.NODE, ObjectType.builder()
                    .setInnerType("id", DataTypes.STRING)
                    .setInnerType("name", DataTypes.STRING)
                    .build())
                .register(Columns.STMT, DataTypes.STRING)
                .register(Columns.STARTED, DataTypes.TIMESTAMPZ),
            PRIMARY_KEY);
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
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
        return Routing.forTableOnAllNodes(IDENT, clusterState.getNodes());
    }
}

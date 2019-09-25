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

import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.job.JobContextLog;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Map;
import java.util.function.Supplier;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.execution.engine.collect.NestableCollectExpression.withNullableProperty;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;
import static io.crate.types.DataTypes.TIMESTAMPZ;

public class SysJobsLogTableInfo extends StaticTableInfo<JobContextLog> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "jobs_log");

    public static Map<ColumnIdent, RowCollectExpressionFactory<JobContextLog>> expressions(Supplier<DiscoveryNode> localNode) {
        return columnRegistrar(localNode).expressions();
    }

    private static ColumnRegistrar<JobContextLog> columnRegistrar(Supplier<DiscoveryNode> localNode) {
        return new ColumnRegistrar<JobContextLog>(IDENT, RowGranularity.DOC)
                  .register("id", STRING, () -> forFunction(log -> log.id().toString()))
                  .register("username", STRING, () -> forFunction(JobContextLog::username))
                  .register("stmt", STRING, () -> forFunction(JobContextLog::statement))
                  .register("started", TIMESTAMPZ, () -> forFunction(JobContextLog::started))
                  .register("ended", TIMESTAMPZ,() -> forFunction(JobContextLog::ended))
                  .register("error", STRING, () -> forFunction(JobContextLog::errorMessage))
                  .register("classification", ObjectType.builder()
                      .setInnerType("type", STRING)
                      .setInnerType("labels", STRING_ARRAY)
                      .build(), () -> withNullableProperty(JobContextLog::classification, c -> ImmutableMap.builder()
                      .put("type", c.type().name())
                      .put("labels", c.labels().toArray(new String[0]))
                      .build()))
            .register("classification","type", STRING,
                () -> withNullableProperty(JobContextLog::classification, c -> c.type().name()))
            .register("classification", "labels", STRING_ARRAY,
                () -> withNullableProperty(JobContextLog::classification, c -> c.labels().toArray(new String[0])))
            .register("node", ObjectType.builder()
                .setInnerType("id", STRING)
                .setInnerType("name", STRING)
                .build(), () -> forFunction(ignored -> Map.of(
                    "id", localNode.get().getId(),
                    "name", localNode.get().getName()
                )))
            .register("node", "id", STRING, () -> forFunction(ignored -> localNode.get().getId()))
            .register("node", "name", STRING, () -> forFunction(ignored -> localNode.get().getName()));
    }

    public SysJobsLogTableInfo(Supplier<DiscoveryNode> localNode) {
        super(IDENT, columnRegistrar(localNode), "id");
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
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

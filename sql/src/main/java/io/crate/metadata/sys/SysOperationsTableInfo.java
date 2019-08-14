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
import io.crate.expression.reference.sys.operation.OperationContext;
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
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.TIMESTAMPZ;

public class SysOperationsTableInfo extends StaticTableInfo<OperationContext> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "operations");

    public static Map<ColumnIdent, RowCollectExpressionFactory<OperationContext>> expressions(Supplier<DiscoveryNode> localNode) {
        return columnRegistrar(localNode).expressions();
    }

    private static ColumnRegistrar<OperationContext> columnRegistrar(Supplier<DiscoveryNode> localNode) {
        return new ColumnRegistrar<OperationContext>(IDENT, RowGranularity.DOC)
            .register("id", STRING, () -> forFunction(c -> String.valueOf(c.id())))
            .register("job_id", STRING, () -> forFunction(c -> c.jobId().toString()))
            .register("name", STRING, () -> forFunction(OperationContext::name))
            .register("started", TIMESTAMPZ, () -> forFunction(OperationContext::started))
            .register("used_bytes", LONG, () -> forFunction(OperationContext::usedBytes))
            .register("node", ObjectType.builder()
                .setInnerType("id", STRING)
                .setInnerType("name", STRING).build(), () -> forFunction(ignored -> ImmutableMap.of(
                "id", localNode.get().getId(),
                "name", localNode.get().getName()
            )))
            .register("node", "id", STRING, () -> forFunction(ignored -> localNode.get().getId()))
            .register("node", "name", STRING, () -> forFunction(ignored -> localNode.get().getName()));
    }

    SysOperationsTableInfo(Supplier<DiscoveryNode> localNode) {
        super(IDENT, columnRegistrar(localNode));
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

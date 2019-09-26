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

import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.expression.reference.sys.shard.ShardSegment;
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
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.LONG;

public class SysSegmentsTableInfo extends StaticTableInfo<ShardSegment> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "segments");

    public static Map<ColumnIdent, RowCollectExpressionFactory<ShardSegment>> expressions(Supplier<DiscoveryNode> localNode) {
        return columnRegistrar(localNode).expressions();
    }

    private static ColumnRegistrar<ShardSegment> columnRegistrar(Supplier<DiscoveryNode> localNode) {
        return new ColumnRegistrar<ShardSegment>(IDENT, RowGranularity.DOC)
            .register("table_schema", STRING, () -> forFunction(r -> r.getIndexParts().getSchema()))
            .register("table_name", STRING, () -> forFunction(r -> r.getIndexParts().getTable()))
            .register("shard_id", INTEGER, () -> forFunction(ShardSegment::getShardId))
            .register("node", ObjectType.builder()
                .setInnerType("id", STRING)
                .setInnerType("name", STRING)
                .build(), () -> forFunction(ignored -> Map.of(
                "id", localNode.get().getId(),
                "name", localNode.get().getName()
            )))
            .register("node", "id", STRING, () -> forFunction(ignored -> localNode.get().getId()))
            .register("node", "name", STRING, () -> forFunction(ignored -> localNode.get().getName()))
            .register("segment_name", STRING, () -> forFunction(r -> r.getSegment().getName()))
            .register("generation", LONG, () -> forFunction(r -> r.getSegment().getGeneration()))
            .register("num_docs", INTEGER, () -> forFunction(r -> r.getSegment().getNumDocs()))
            .register("deleted_docs", INTEGER, () -> forFunction(r -> r.getSegment().getDeletedDocs()))
            .register("size", LONG, () -> forFunction(r -> r.getSegment().sizeInBytes))
            .register("memory", LONG, () -> forFunction(r -> r.getSegment().memoryInBytes))
            .register("committed", BOOLEAN, () -> forFunction(r -> r.getSegment().committed))
            .register("primary", BOOLEAN, () -> forFunction(ShardSegment::primary))
            .register("search", BOOLEAN, () -> forFunction(r -> r.getSegment().search))
            .register("version", STRING, () -> forFunction(r -> r.getSegment().getVersion().toString()))
            .register("compound", BOOLEAN, () -> forFunction(r -> r.getSegment().compound))
            .register("attributes", ObjectType.untyped(), () -> forFunction(r -> r.getSegment().getAttributes()));
    }

    SysSegmentsTableInfo(Supplier<DiscoveryNode> localNode) {
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

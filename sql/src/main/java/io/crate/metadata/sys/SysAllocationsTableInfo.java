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
import io.crate.expression.reference.sys.shard.SysAllocation;
import io.crate.expression.reference.sys.shard.SysAllocationDecisionsExpression;
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

import java.util.HashMap;
import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

public class SysAllocationsTableInfo extends StaticTableInfo<SysAllocation> {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "allocations");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    public static class Columns {
        static final ColumnIdent DECISIONS_NODE_ID = new ColumnIdent("decisions", "node_id");
        static final ColumnIdent DECISIONS_NODE_NAME = new ColumnIdent("decisions", "node_name");
        static final ColumnIdent DECISIONS_EXPLANATIONS = new ColumnIdent("decisions", "explanations");
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<SysAllocation>> expressions() {
        return columnRegistrar().expressions();
    }

    @SuppressWarnings({"unchecked"})
    private static ColumnRegistrar<SysAllocation> columnRegistrar() {
        return new ColumnRegistrar<SysAllocation>(IDENT, GRANULARITY)
            .register("table_schema", STRING, () -> forFunction(SysAllocation::tableSchema))
            .register("table_name", STRING, () -> forFunction(SysAllocation::tableName))
            .register("partition_ident", STRING, () -> forFunction(SysAllocation::partitionIdent))
            .register("shard_id", INTEGER, () -> forFunction(SysAllocation::shardId))
            .register("node_id", STRING, () -> forFunction(SysAllocation::nodeId))
            .register("primary", BOOLEAN, () -> forFunction(SysAllocation::primary))
            .register("current_state", STRING, () -> forFunction(s -> s.currentState().toString()))
            .register("explanation", STRING, () -> forFunction(SysAllocation::explanation))
            .register("decisions", ObjectType.builder()
                .setInnerType("node_id", STRING)
                .setInnerType("node_name", STRING)
                .setInnerType("explanations", STRING_ARRAY)
            .build(), () -> new SysAllocationDecisionsExpression<Map<String, Object>>() {
                @Override
                protected Map<String, Object> valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                    var decision = new HashMap<String, Object>(3);
                    decision.put(Columns.DECISIONS_NODE_ID.path().get(0), input.nodeId());
                    decision.put(Columns.DECISIONS_NODE_NAME.path().get(0), input.nodeName());
                    decision.put(Columns.DECISIONS_EXPLANATIONS.path().get(0), input.explanations());
                    return decision;
                }
            })
            .register("decisions","node_id", STRING, () -> new SysAllocationDecisionsExpression<String>() {

                @Override
                protected String valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                    return input.nodeId();
                }
            })
            .register("decisions", "node_name", STRING, () -> new SysAllocationDecisionsExpression<String>() {

                @Override
                protected String valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                    return input.nodeName();
                }
            })
            .register("decisions","explanations", STRING_ARRAY, () -> new SysAllocationDecisionsExpression<String[]>() {

                @Override
                protected String[] valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                    return input.explanations();
                }
            });

    }

    SysAllocationsTableInfo() {
        super(IDENT, columnRegistrar(), "table_schema", "table_name", "partition_ident", "shard_id");
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(ClusterState state, RoutingProvider routingProvider, WhereClause whereClause, RoutingProvider.ShardSelection shardSelection, SessionContext sessionContext) {
        return Routing.forTableOnSingleNode(IDENT, state.getNodes().getLocalNodeId());
    }
}

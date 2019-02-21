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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.execution.engine.collect.NestableCollectExpression;
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
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.cluster.ClusterState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SysAllocationsTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "allocations");
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;
    private static final List<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(Columns.TABLE_SCHEMA,
        Columns.TABLE_NAME, Columns.PARTITION_IDENT, Columns.SHARD_ID);

    public static class Columns {
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        static final ColumnIdent SHARD_ID = new ColumnIdent("shard_id");
        static final ColumnIdent NODE_ID = new ColumnIdent("node_id");
        static final ColumnIdent PRIMARY = new ColumnIdent("primary");
        static final ColumnIdent CURRENT_STATE = new ColumnIdent("current_state");
        static final ColumnIdent EXPLANATION = new ColumnIdent("explanation");
        static final ColumnIdent DECISIONS = new ColumnIdent("decisions");
        static final ColumnIdent DECISIONS_NODE_ID = new ColumnIdent("decisions", "node_id");
        static final ColumnIdent DECISIONS_NODE_NAME = new ColumnIdent("decisions", "node_name");
        static final ColumnIdent DECISIONS_EXPLANATIONS = new ColumnIdent("decisions", "explanations");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<SysAllocation>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<SysAllocation>>builder()
            .put(Columns.TABLE_SCHEMA,
                () -> NestableCollectExpression.forFunction(SysAllocation::tableSchema))
            .put(Columns.TABLE_NAME,
                () -> NestableCollectExpression.forFunction(SysAllocation::tableName))
            .put(Columns.PARTITION_IDENT,
                () -> NestableCollectExpression.forFunction(SysAllocation::partitionIdent))
            .put(Columns.SHARD_ID,
                () -> NestableCollectExpression.forFunction(SysAllocation::shardId))
            .put(Columns.NODE_ID,
                () -> NestableCollectExpression.forFunction(SysAllocation::nodeId))
            .put(Columns.PRIMARY,
                () -> NestableCollectExpression.forFunction(SysAllocation::primary))
            .put(Columns.CURRENT_STATE,
                () -> NestableCollectExpression.forFunction(s -> s.currentState().toString()))
            .put(Columns.EXPLANATION,
                () -> NestableCollectExpression.forFunction(SysAllocation::explanation))
            .put(Columns.DECISIONS,
                () -> new SysAllocationDecisionsExpression<Map<String, Object>>() {

                    @Override
                    protected Map<String, Object> valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                        Map<String, Object> decision = new HashMap<>(3);
                        decision.put(Columns.DECISIONS_NODE_ID.path().get(0), input.nodeId());
                        decision.put(Columns.DECISIONS_NODE_NAME.path().get(0), input.nodeName());
                        decision.put(Columns.DECISIONS_EXPLANATIONS.path().get(0), input.explanations());
                        return decision;
                    }
                })
            .put(Columns.DECISIONS_NODE_ID, () -> new SysAllocationDecisionsExpression<String>() {

                    @Override
                    protected String valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                        return input.nodeId();
                    }
                })
            .put(Columns.DECISIONS_NODE_NAME, () -> new SysAllocationDecisionsExpression<String>() {

                    @Override
                    protected String valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                        return input.nodeName();
                    }
                })
            .put(Columns.DECISIONS_EXPLANATIONS, () -> new SysAllocationDecisionsExpression<String[]>() {

                    @Override
                    protected String[] valueForItem(SysAllocation.SysAllocationNodeDecision input) {
                        return input.explanations();
                    }
                })
            .build();
    }

    SysAllocationsTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
            .register(Columns.TABLE_NAME, DataTypes.STRING)
            .register(Columns.PARTITION_IDENT, DataTypes.STRING)
            .register(Columns.SHARD_ID, DataTypes.INTEGER)
            .register(Columns.NODE_ID, DataTypes.STRING)
            .register(Columns.PRIMARY, DataTypes.BOOLEAN)
            .register(Columns.CURRENT_STATE, DataTypes.STRING)
            .register(Columns.EXPLANATION, DataTypes.STRING)
            .register(Columns.DECISIONS, new ArrayType(ObjectType.builder()
                .setInnerType("node_id", DataTypes.STRING)
                .setInnerType("node_name", DataTypes.STRING)
                .setInnerType("explanations", DataTypes.STRING_ARRAY)
                .build())),
            PRIMARY_KEYS);
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

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.WhereClause;
import io.crate.analyze.user.Privilege;
import io.crate.auth.user.User;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.sys.shard.NodeNestableInput;
import io.crate.expression.reference.sys.shard.ShardMinLuceneVersionExpression;
import io.crate.expression.reference.sys.shard.ShardNumDocsExpression;
import io.crate.expression.reference.sys.shard.ShardPartitionOrphanedExpression;
import io.crate.expression.reference.sys.shard.ShardRecoveryExpression;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexParts;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RoutingProvider;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.BooleanType;
import io.crate.types.DataTypes;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ObjectType;
import io.crate.types.StringType;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SysShardsTableInfo extends StaticTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "shards");

    public static class Columns {
        /**
         * Implementations have to be registered in
         *  - {@link #expressions()}
         *  - {@link #unassignedShardsExpressions()}
         */

        public static final ColumnIdent ID = new ColumnIdent("id");
        static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        static final ColumnIdent NUM_DOCS = new ColumnIdent("num_docs");
        public static final ColumnIdent PRIMARY = new ColumnIdent("primary");
        static final ColumnIdent RELOCATING_NODE = new ColumnIdent("relocating_node");
        public static final ColumnIdent SIZE = new ColumnIdent("size");
        static final ColumnIdent STATE = new ColumnIdent("state");
        static final ColumnIdent ROUTING_STATE = new ColumnIdent("routing_state");
        static final ColumnIdent ORPHAN_PARTITION = new ColumnIdent("orphan_partition");

        static final ColumnIdent RECOVERY = new ColumnIdent("recovery");
        static final ColumnIdent RECOVERY_STAGE = new ColumnIdent("recovery", ImmutableList.of("stage"));
        static final ColumnIdent RECOVERY_TYPE = new ColumnIdent("recovery", ImmutableList.of("type"));
        static final ColumnIdent RECOVERY_TOTAL_TIME =
            new ColumnIdent("recovery", ImmutableList.of("total_time"));

        static final ColumnIdent RECOVERY_FILES = new ColumnIdent("recovery", ImmutableList.of("files"));
        static final ColumnIdent RECOVERY_FILES_USED =
            new ColumnIdent("recovery", ImmutableList.of("files", "used"));
        static final ColumnIdent RECOVERY_FILES_REUSED =
            new ColumnIdent("recovery", ImmutableList.of("files", "reused"));
        static final ColumnIdent RECOVERY_FILES_RECOVERED =
            new ColumnIdent("recovery", ImmutableList.of("files", "recovered"));
        static final ColumnIdent RECOVERY_FILES_PERCENT =
            new ColumnIdent("recovery", ImmutableList.of("files", "percent"));

        static final ColumnIdent RECOVERY_SIZE =
            new ColumnIdent("recovery", ImmutableList.of("size"));
        static final ColumnIdent RECOVERY_SIZE_USED =
            new ColumnIdent("recovery", ImmutableList.of("size", "used"));
        static final ColumnIdent RECOVERY_SIZE_REUSED =
            new ColumnIdent("recovery", ImmutableList.of("size", "reused"));
        static final ColumnIdent RECOVERY_SIZE_RECOVERED =
            new ColumnIdent("recovery", ImmutableList.of("size", "recovered"));
        static final ColumnIdent RECOVERY_SIZE_PERCENT =
            new ColumnIdent("recovery", ImmutableList.of("size", "percent"));

        static final ColumnIdent PATH = new ColumnIdent("path");
        static final ColumnIdent BLOB_PATH = new ColumnIdent("blob_path");

        static final ColumnIdent MIN_LUCENE_VERSION = new ColumnIdent("min_lucene_version");
        static final ColumnIdent NODE = new ColumnIdent("node");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ShardRowContext>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ShardRowContext>>builder()
            .put(Columns.SCHEMA_NAME,
                () -> NestableCollectExpression.forFunction(r -> r.indexParts().getSchema()))
            .put(Columns.TABLE_NAME,
                () -> NestableCollectExpression.forFunction(r -> r.indexParts().getTable()))
            .put(Columns.PARTITION_IDENT,
                () -> NestableCollectExpression.forFunction(ShardRowContext::partitionIdent))
            .put(Columns.ID, () -> NestableCollectExpression.forFunction(ShardRowContext::id))
            .put(Columns.NUM_DOCS, ShardNumDocsExpression::new)
            .put(Columns.PRIMARY,
                () -> NestableCollectExpression.forFunction(r -> r.indexShard().routingEntry().primary()))
            .put(Columns.RELOCATING_NODE,
                () -> NestableCollectExpression.forFunction(r -> r.indexShard().routingEntry().relocatingNodeId()))
            .put(Columns.SIZE,
                () -> NestableCollectExpression.forFunction(ShardRowContext::size))
            .put(Columns.STATE,
                () -> NestableCollectExpression.forFunction(r -> r.indexShard().state().toString()))
            .put(Columns.ROUTING_STATE,
                () -> NestableCollectExpression.forFunction(r -> r.indexShard().routingEntry().state().toString()))
            .put(Columns.ORPHAN_PARTITION, ShardPartitionOrphanedExpression::new)
            .put(Columns.RECOVERY, ShardRecoveryExpression::new)
            .put(Columns.PATH, () -> NestableCollectExpression.forFunction(ShardRowContext::path))
            .put(Columns.BLOB_PATH, () -> NestableCollectExpression.forFunction(ShardRowContext::blobPath))
            .put(Columns.MIN_LUCENE_VERSION, ShardMinLuceneVersionExpression::new)
            .put(Columns.NODE, NodeNestableInput::new)
            .build();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<UnassignedShard>> unassignedShardsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<UnassignedShard>>builder()
            .put(Columns.SCHEMA_NAME,
                () -> NestableCollectExpression.forFunction(UnassignedShard::schemaName))
            .put(Columns.TABLE_NAME,
                () -> NestableCollectExpression.forFunction(UnassignedShard::tableName))
            .put(Columns.PARTITION_IDENT,
                () -> NestableCollectExpression.forFunction(UnassignedShard::partitionIdent))
            .put(Columns.ID,
                () -> NestableCollectExpression.forFunction(UnassignedShard::id))
            .put(Columns.NUM_DOCS,
                () -> NestableCollectExpression.constant(0L))
            .put(Columns.PRIMARY,
                () -> NestableCollectExpression.forFunction(UnassignedShard::primary))
            .put(Columns.RELOCATING_NODE,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.SIZE,
                () -> NestableCollectExpression.constant(0L))
            .put(Columns.STATE,
                () -> NestableCollectExpression.forFunction(UnassignedShard::state))
            .put(Columns.ROUTING_STATE,
                () -> NestableCollectExpression.forFunction(UnassignedShard::state))
            .put(Columns.ORPHAN_PARTITION,
                () -> NestableCollectExpression.forFunction(UnassignedShard::orphanedPartition))
            .put(Columns.RECOVERY, () -> new NestableCollectExpression<>() {
                @Override
                public void setNextRow(UnassignedShard unassignedShard) {
                }

                @Override
                public Object value() {
                    return null;
                }

                @Override
                public NestableInput getChild(String name) {
                    return this;
                }
            })
            .put(Columns.PATH,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.BLOB_PATH,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.MIN_LUCENE_VERSION,
                () -> NestableCollectExpression.constant(null))
            .put(Columns.NODE, () -> new NestableCollectExpression<>() {
                @Override
                public void setNextRow(UnassignedShard unassignedShard) {
                }

                @Override
                public Object value() {
                    return null; // unassigned shards are on *no* node.
                }

                @Override
                public NestableInput<?> getChild(String name) {
                    return this;
                }
            })
            .build();
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(
        Columns.SCHEMA_NAME,
        Columns.TABLE_NAME,
        Columns.ID,
        Columns.PARTITION_IDENT
    );

    private static final ObjectType TYPE_RECOVERY_SIZE = ObjectType.builder()
        .setInnerType("used", LongType.INSTANCE)
        .setInnerType("reused", LongType.INSTANCE)
        .setInnerType("recovered", LongType.INSTANCE)
        .setInnerType("percent", FloatType.INSTANCE)
        .build();

    private static final ObjectType TYPE_RECOVERY_FILES = ObjectType.builder()
        .setInnerType("used", IntegerType.INSTANCE)
        .setInnerType("reused", IntegerType.INSTANCE)
        .setInnerType("recovered", IntegerType.INSTANCE)
        .setInnerType("percent", FloatType.INSTANCE)
        .build();

    SysShardsTableInfo() {
        super(IDENT, new ColumnRegistrar(IDENT, RowGranularity.SHARD)
                .register(Columns.SCHEMA_NAME, StringType.INSTANCE)
                .register(Columns.TABLE_NAME, StringType.INSTANCE)
                .register(Columns.ID, IntegerType.INSTANCE)
                .register(Columns.PARTITION_IDENT, StringType.INSTANCE)
                .register(Columns.NUM_DOCS, LongType.INSTANCE)
                .register(Columns.PRIMARY, BooleanType.INSTANCE)
                .register(Columns.RELOCATING_NODE, StringType.INSTANCE)
                .register(Columns.SIZE, LongType.INSTANCE)
                .register(Columns.STATE, StringType.INSTANCE)
                .register(Columns.ROUTING_STATE, StringType.INSTANCE)
                .register(Columns.ORPHAN_PARTITION, BooleanType.INSTANCE)
                .register(Columns.RECOVERY, ObjectType.builder()
                    .setInnerType("stage", StringType.INSTANCE)
                    .setInnerType("type", StringType.INSTANCE)
                    .setInnerType("total_time", LongType.INSTANCE)
                    .setInnerType("size", TYPE_RECOVERY_SIZE)
                    .setInnerType("files", TYPE_RECOVERY_FILES)
                    .build())
                .register(Columns.PATH, DataTypes.STRING)
                .register(Columns.BLOB_PATH, DataTypes.STRING)
                .register(Columns.MIN_LUCENE_VERSION, StringType.INSTANCE)
                .register(Columns.NODE, ObjectType.builder()
                    .setInnerType("id", DataTypes.STRING)
                    .setInnerType("name", DataTypes.STRING)
                    .build()),
            PRIMARY_KEY);
    }

    private void processShardRouting(String localNodeId,
                                     Map<String, Map<String, IntIndexedContainer>> routing,
                                     ShardRouting shardRouting,
                                     ShardId shardId) {
        String node;
        int id;
        String index = shardId.getIndex().getName();

        if (shardRouting == null) {
            node = localNodeId;
            id = UnassignedShard.markUnassigned(shardId.id());
        } else {
            node = shardRouting.currentNodeId();
            id = shardRouting.id();
        }
        Map<String, IntIndexedContainer> nodeMap = routing.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            routing.put(node, nodeMap);
        }

        IntIndexedContainer shards = nodeMap.get(index);
        if (shards == null) {
            shards = new IntArrayList();
            nodeMap.put(index, shards);
        }
        shards.add(id);
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.SHARD;
    }

    /**
     * Retrieves the routing for sys.shards
     * <p>
     * This routing contains ALL shards of ALL indices.
     * Any shards that are not yet assigned to a node will have a NEGATIVE shard id (see {@link UnassignedShard}
     */
    @Override
    public Routing getRouting(ClusterState clusterState,
                              RoutingProvider routingProvider,
                              WhereClause whereClause,
                              RoutingProvider.ShardSelection shardSelection,
                              SessionContext sessionContext) {
        // TODO: filter on whereClause
        String[] concreteIndices = Arrays.stream(clusterState.metaData().getConcreteAllOpenIndices())
            .filter(index -> !IndexParts.isDangling(index))
            .toArray(String[]::new);
        User user = sessionContext != null ? sessionContext.user() : null;
        if (user != null) {
            List<String> accessibleTables = new ArrayList<>(concreteIndices.length);
            for (String indexName : concreteIndices) {
                String tableName = RelationName.fqnFromIndexName(indexName);
                if (user.hasAnyPrivilege(Privilege.Clazz.TABLE, tableName)) {
                    accessibleTables.add(indexName);
                }
            }
            concreteIndices = accessibleTables.toArray(new String[0]);
        }

        Map<String, Map<String, IntIndexedContainer>> locations = new TreeMap<>();
        GroupShardsIterator<ShardIterator> groupShardsIterator =
            clusterState.getRoutingTable().allAssignedShardsGrouped(concreteIndices, true, true);
        for (final ShardIterator shardIt : groupShardsIterator) {
            final ShardRouting shardRouting = shardIt.nextOrNull();
            processShardRouting(clusterState.getNodes().getLocalNodeId(), locations, shardRouting, shardIt.shardId());
        }
        return new Routing(locations);
    }
}

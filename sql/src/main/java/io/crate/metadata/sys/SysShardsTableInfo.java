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
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.ObjectType;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.metadata.table.ColumnRegistrar.entry;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;

public class SysShardsTableInfo extends StaticTableInfo<ShardRowContext> {

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

        static final ColumnIdent PATH = new ColumnIdent("path");
        static final ColumnIdent BLOB_PATH = new ColumnIdent("blob_path");

        static final ColumnIdent MIN_LUCENE_VERSION = new ColumnIdent("min_lucene_version");
        static final ColumnIdent NODE = new ColumnIdent("node");
        static final ColumnIdent SEQ_NO_STATS = new ColumnIdent("seq_no_stats");
        static final ColumnIdent TRANSLOG_STATS = new ColumnIdent("translog_stats");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ShardRowContext>> expressions() {
        return columnRegistrar().expressions();
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<UnassignedShard>> unassignedShardsExpressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<UnassignedShard>>builder()
            .put(Columns.SCHEMA_NAME, () -> forFunction(UnassignedShard::schemaName))
            .put(Columns.TABLE_NAME, () -> forFunction(UnassignedShard::tableName))
            .put(Columns.PARTITION_IDENT, () -> forFunction(UnassignedShard::partitionIdent))
            .put(Columns.ID, () -> forFunction(UnassignedShard::id))
            .put(Columns.NUM_DOCS, () -> constant(0L))
            .put(Columns.PRIMARY, () -> forFunction(UnassignedShard::primary))
            .put(Columns.RELOCATING_NODE, () -> constant(null))
            .put(Columns.SIZE, () -> constant(0L))
            .put(Columns.STATE, () -> forFunction(UnassignedShard::state))
            .put(Columns.ROUTING_STATE, () -> forFunction(UnassignedShard::state))
            .put(Columns.ORPHAN_PARTITION, () -> forFunction(UnassignedShard::orphanedPartition))
            .put(Columns.RECOVERY, NestedNullObjectExpression::new)
            .put(Columns.PATH, () -> constant(null))
            .put(Columns.BLOB_PATH, () -> constant(null))
            .put(Columns.MIN_LUCENE_VERSION, () -> constant(null))
            .put(Columns.NODE, NestedNullObjectExpression::new)
            .put(Columns.SEQ_NO_STATS, NestedNullObjectExpression::new)
            .put(Columns.TRANSLOG_STATS, NestedNullObjectExpression::new)
            .build();
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(
        Columns.SCHEMA_NAME,
        Columns.TABLE_NAME,
        Columns.ID,
        Columns.PARTITION_IDENT
    );

    private static final ObjectType TYPE_RECOVERY_SIZE = ObjectType.builder()
        .setInnerType("used", LONG)
        .setInnerType("reused", LONG)
        .setInnerType("recovered", LONG)
        .setInnerType("percent", FloatType.INSTANCE)
        .build();

    private static final ObjectType TYPE_RECOVERY_FILES = ObjectType.builder()
        .setInnerType("used", IntegerType.INSTANCE)
        .setInnerType("reused", IntegerType.INSTANCE)
        .setInnerType("recovered", IntegerType.INSTANCE)
        .setInnerType("percent", FloatType.INSTANCE)
        .build();

    private static ColumnRegistrar<ShardRowContext> columnRegistrar() {
        return new ColumnRegistrar<ShardRowContext>(IDENT, RowGranularity.SHARD)
            .register("schema_name", STRING, () -> forFunction(r -> r.indexParts().getSchema()))
            .register("table_name", STRING, () -> forFunction(r -> r.indexParts().getTable()))
            .register("id", INTEGER, () -> forFunction(ShardRowContext::id))
            .register("partition_ident", STRING,() -> forFunction(ShardRowContext::partitionIdent))
            .register("num_docs", LONG, ShardNumDocsExpression::new)
            .register("primary", BOOLEAN, () -> forFunction(r -> r.indexShard().routingEntry().primary()))
            .register("relocating_node", STRING, () -> forFunction(r -> r.indexShard().routingEntry().relocatingNodeId()))
            .register("size", LONG, () -> forFunction(ShardRowContext::size))
            .register("state", STRING, () -> forFunction(r -> r.indexShard().state().toString()))
            .register("routing_state", STRING,() -> forFunction(r -> r.indexShard().routingEntry().state().toString()))
            .register("orphan_partition", BOOLEAN, ShardPartitionOrphanedExpression::new)
            .register("recovery", ObjectType.builder()
                .setInnerType("stage", STRING)
                .setInnerType("type", STRING)
                .setInnerType("total_time", LONG)
                .setInnerType("size", TYPE_RECOVERY_SIZE)
                .setInnerType("files", TYPE_RECOVERY_FILES)
                .build(), ShardRecoveryExpression::new)
            .register("path", STRING, () -> forFunction(ShardRowContext::path))
            .register("blob_path", STRING,() -> forFunction(ShardRowContext::blobPath))
            .register("min_lucene_version", STRING, ShardMinLuceneVersionExpression::new)
            .register("node", ObjectType.builder()
                .setInnerType("id", STRING)
                .setInnerType("name", STRING)
                .build(), NodeNestableInput::new)
            .register(
                Columns.SEQ_NO_STATS.name(),
                ColumnRegistrar.object(
                    entry(SeqNoStats.MAX_SEQ_NO, LONG, orDefaultIfClosed(r -> r.indexShard().seqNoStats().getMaxSeqNo(), 0L)),
                    entry(SeqNoStats.LOCAL_CHECKPOINT, LONG, orDefaultIfClosed(r -> r.indexShard().seqNoStats().getLocalCheckpoint(), 0L)),
                    entry(SeqNoStats.GLOBAL_CHECKPOINT, LONG, orDefaultIfClosed(r -> r.indexShard().seqNoStats().getGlobalCheckpoint(), 0L))
                )
            )
            .register(
                Columns.TRANSLOG_STATS.name(),
                ColumnRegistrar.object(
                    entry("size", LONG, orDefaultIfClosed(r -> r.indexShard().translogStats().getTranslogSizeInBytes(), 0L)),
                    entry("uncommitted_size", LONG, orDefaultIfClosed(r -> r.indexShard().translogStats().getUncommittedSizeInBytes(), 0L)),
                    entry("number_of_operations", INTEGER, orDefaultIfClosed(r -> r.indexShard().translogStats().estimatedNumberOfOperations(), 0)),
                    entry("uncommitted_operations", INTEGER, orDefaultIfClosed(r -> r.indexShard().translogStats().getUncommittedOperations(), 0))
                )
            );
    }

    private static <T, U> Function<T, U> orDefaultIfClosed(Function<T, U> getProperty, U defaultVal) {
        return x -> {
            try {
                return getProperty.apply(x);
            } catch (AlreadyClosedException e) {
                return defaultVal;
            }
        };
    }

    SysShardsTableInfo() {
        super(IDENT, columnRegistrar(), PRIMARY_KEY);
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

    private static class NestedNullObjectExpression implements NestableCollectExpression<UnassignedShard, Object> {

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
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import static io.crate.execution.engine.collect.NestableCollectExpression.constant;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.LONG;
import static io.crate.types.DataTypes.STRING;
import static java.util.Map.entry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.shard.ShardId;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexParts;
import io.crate.metadata.RelationName;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SystemTable;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.role.Privilege;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

public class SysShardsTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "shards");

    public static class Columns {
        /**
         * Implementations have to be registered in
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
        static final ColumnIdent CLOSED = new ColumnIdent("closed");
        static final ColumnIdent ROUTING_STATE = new ColumnIdent("routing_state");
        static final ColumnIdent ORPHAN_PARTITION = new ColumnIdent("orphan_partition");

        static final ColumnIdent RECOVERY = new ColumnIdent("recovery");

        static final ColumnIdent PATH = new ColumnIdent("path");
        static final ColumnIdent BLOB_PATH = new ColumnIdent("blob_path");

        static final ColumnIdent MIN_LUCENE_VERSION = new ColumnIdent("min_lucene_version");
        static final ColumnIdent NODE = new ColumnIdent("node");
        static final ColumnIdent SEQ_NO_STATS = new ColumnIdent("seq_no_stats");
        static final ColumnIdent TRANSLOG_STATS = new ColumnIdent("translog_stats");
        static final ColumnIdent RETENTION_LEASES = new ColumnIdent("retention_leases");
        static final ColumnIdent FLUSH_STATS = new ColumnIdent("flush_stats");
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<UnassignedShard>> unassignedShardsExpressions() {
        return Map.ofEntries(
            entry(Columns.SCHEMA_NAME, () -> forFunction(UnassignedShard::schemaName)),
            entry(Columns.TABLE_NAME, () -> forFunction(UnassignedShard::tableName)),
            entry(Columns.PARTITION_IDENT, () -> forFunction(UnassignedShard::partitionIdent)),
            entry(Columns.ID, () -> forFunction(UnassignedShard::id)),
            entry(Columns.NUM_DOCS, () -> constant(0L)),
            entry(Columns.PRIMARY, () -> forFunction(UnassignedShard::primary)),
            entry(Columns.RELOCATING_NODE, () -> constant(null)),
            entry(Columns.SIZE, () -> constant(0L)),
            entry(Columns.STATE, () -> forFunction(UnassignedShard::state)),
            entry(Columns.CLOSED, () -> constant(null)),
            entry(Columns.ROUTING_STATE, () -> forFunction(UnassignedShard::state)),
            entry(Columns.ORPHAN_PARTITION, () -> forFunction(UnassignedShard::orphanedPartition)),
            entry(Columns.RECOVERY, NestedNullObjectExpression::new),
            entry(Columns.PATH, () -> constant(null)),
            entry(Columns.BLOB_PATH, () -> constant(null)),
            entry(Columns.MIN_LUCENE_VERSION, () -> constant(null)),
            entry(Columns.NODE, NestedNullObjectExpression::new),
            entry(Columns.SEQ_NO_STATS, NestedNullObjectExpression::new),
            entry(Columns.TRANSLOG_STATS, NestedNullObjectExpression::new),
            entry(Columns.RETENTION_LEASES, NestedNullObjectExpression::new),
            entry(Columns.FLUSH_STATS, NestedNullObjectExpression::new)
        );
    }

    public static SystemTable<ShardRowContext> create(Roles roles) {
        return SystemTable.<ShardRowContext>builder(IDENT, RowGranularity.SHARD)
            .add("schema_name", STRING, r -> r.indexParts().getSchema())
            .add("table_name", STRING, r -> r.indexParts().getTable())
            .add("id", INTEGER, ShardRowContext::id)
            .add("partition_ident", STRING, ShardRowContext::partitionIdent)
            .add("num_docs", LONG, ShardRowContext::numDocs)
            .add("primary", BOOLEAN, r -> r.indexShard().routingEntry().primary())
            .add("relocating_node", STRING, r -> r.indexShard().routingEntry().relocatingNodeId())
            .add("size", LONG, ShardRowContext::size)
            .add("state", STRING, r -> r.indexShard().state().toString())
            .add("closed", BOOLEAN, ShardRowContext::isClosed)
            .add("routing_state", STRING,r -> r.indexShard().routingEntry().state().toString())
            .add("orphan_partition", BOOLEAN, ShardRowContext::isOrphanedPartition)

            .startObject("recovery")
                .add("stage", STRING, ShardRowContext::recoveryStage)
                .add("type", STRING, ShardRowContext::recoveryType)
                .add("total_time", LONG, ShardRowContext::recoveryTotalTime)

                .startObject("size")
                    .add("used", LONG, ShardRowContext::recoverySizeUsed)
                    .add("reused", LONG, ShardRowContext::recoverySizeReused)
                    .add("recovered", LONG, ShardRowContext::recoverySizeRecoveredBytes)
                    .add("percent", DataTypes.FLOAT, ShardRowContext::recoverySizeRecoveredBytesPercent)
                .endObject()

                .startObject("files")
                    .add("used", INTEGER, ShardRowContext::recoveryFilesUsed)
                    .add("reused", INTEGER, ShardRowContext::recoveryFilesReused)
                    .add("recovered", INTEGER, ShardRowContext::recoveryFilesRecovered)
                    .add("percent", DataTypes.FLOAT, ShardRowContext::recoveryFilesPercent)
                .endObject()

            .endObject()

            .add("path", STRING, ShardRowContext::path)
            .add("blob_path", STRING, ShardRowContext::blobPath)
            .add("min_lucene_version", STRING, ShardRowContext::minLuceneVersion)
            .startObject("node")
                .add("id", STRING, x -> x.clusterService().localNode().getId())
                .add("name", STRING, x -> x.clusterService().localNode().getName())
            .endObject()
            .startObject(Columns.SEQ_NO_STATS.name())
                .add(SeqNoStats.MAX_SEQ_NO, LONG, ShardRowContext::maxSeqNo)
                .add(SeqNoStats.LOCAL_CHECKPOINT, LONG, ShardRowContext::localSeqNoCheckpoint)
                .add(SeqNoStats.GLOBAL_CHECKPOINT, LONG, ShardRowContext::globalSeqNoCheckpoint)
            .endObject()
            .startObject(Columns.TRANSLOG_STATS.name())
                .add("size", LONG, ShardRowContext::translogSizeInBytes)
                .add("uncommitted_size", LONG, ShardRowContext::translogUncommittedSizeInBytes)
                .add("number_of_operations", INTEGER, ShardRowContext::translogEstimatedNumberOfOperations)
                .add("uncommitted_operations", INTEGER, ShardRowContext::translogUncommittedOperations)
            .endObject()
            .startObject(Columns.RETENTION_LEASES.name())
                .add("primary_term", LONG, ShardRowContext::retentionLeasesPrimaryTerm)
                .add("version", LONG, ShardRowContext::retentionLeasesVersion)
                .startObjectArray("leases", ShardRowContext::retentionLeases)
                    .add("id", STRING, RetentionLease::id)
                    .add("retaining_seq_no", LONG, RetentionLease::retainingSequenceNumber)
                    .add("timestamp", DataTypes.TIMESTAMPZ, RetentionLease::timestamp)
                    .add("source", STRING, RetentionLease::source)
                .endObjectArray()
            .endObject()
            .startObject(Columns.FLUSH_STATS.name())
                .add("count", LONG, ShardRowContext::flushCount)
                .add("periodic_count", LONG, ShardRowContext::flushPeriodicCount)
                .add("total_time_ns", LONG, ShardRowContext::flushTotalTimeNs)
            .endObject()
            .setPrimaryKeys(
                Columns.SCHEMA_NAME,
                Columns.TABLE_NAME,
                Columns.ID,
                Columns.PARTITION_IDENT
            )
            .withRouting((state, routingProvider, sessionSettings) ->
                getRouting(state, sessionSettings, roles))
            .build();
    }

    private static void processShardRouting(String localNodeId,
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
        Map<String, IntIndexedContainer> nodeMap = routing.computeIfAbsent(node, k -> new TreeMap<>());

        IntIndexedContainer shards = nodeMap.get(index);
        if (shards == null) {
            shards = new IntArrayList();
            nodeMap.put(index, shards);
        }
        shards.add(id);
    }

    /**
     * Retrieves the routing for sys.shards
     * <p>
     * This routing contains ALL shards of ALL indices.
     * Any shards that are not yet assigned to a node will have a NEGATIVE shard id (see {@link UnassignedShard}
     */
    public static Routing getRouting(ClusterState clusterState,
                                     CoordinatorSessionSettings sessionSettings,
                                     Roles roles) {
        String[] concreteIndices = Arrays.stream(clusterState.metadata().getConcreteAllIndices())
            .filter(index -> !IndexParts.isDangling(index))
            .toArray(String[]::new);
        Role user = sessionSettings != null ? sessionSettings.sessionUser() : null;
        if (user != null) {
            List<String> accessibleTables = new ArrayList<>(concreteIndices.length);
            for (String indexName : concreteIndices) {
                String tableName = RelationName.fqnFromIndexName(indexName);
                if (roles.hasAnyPrivilege(user, Privilege.Securable.TABLE, tableName)) {
                    accessibleTables.add(indexName);
                }
            }
            concreteIndices = accessibleTables.toArray(new String[0]);
        }

        Map<String, Map<String, IntIndexedContainer>> locations = new TreeMap<>();
        GroupShardsIterator<ShardIterator> groupShardsIterator =
            clusterState.routingTable().allAssignedShardsGrouped(concreteIndices, true);
        for (final ShardIterator shardIt : groupShardsIterator) {
            final ShardRouting shardRouting = shardIt.nextOrNull();
            processShardRouting(clusterState.nodes().getLocalNodeId(), locations, shardRouting, shardIt.shardId());
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
        public NestableInput<?> getChild(String name) {
            return this;
        }
    }
}

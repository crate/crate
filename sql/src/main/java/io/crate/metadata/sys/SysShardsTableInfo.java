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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.*;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Singleton
public class SysShardsTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "shards");
    private final ClusterService service;

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        public static final ColumnIdent NUM_DOCS = new ColumnIdent("num_docs");
        public static final ColumnIdent PRIMARY = new ColumnIdent("primary");
        public static final ColumnIdent RELOCATING_NODE = new ColumnIdent("relocating_node");
        public static final ColumnIdent SIZE = new ColumnIdent("size");
        public static final ColumnIdent STATE = new ColumnIdent("state");
        public static final ColumnIdent ROUTING_STATE = new ColumnIdent("routing_state");
        public static final ColumnIdent ORPHAN_PARTITION = new ColumnIdent("orphan_partition");

        public static final ColumnIdent RECOVERY = new ColumnIdent("recovery");
        public static final ColumnIdent RECOVERY_STAGE = new ColumnIdent("recovery", ImmutableList.of("stage"));
        public static final ColumnIdent RECOVERY_TYPE = new ColumnIdent("recovery", ImmutableList.of("type"));
        public static final ColumnIdent RECOVERY_TOTAL_TIME =
            new ColumnIdent("recovery", ImmutableList.of("total_time"));

        public static final ColumnIdent RECOVERY_FILES = new ColumnIdent("recovery", ImmutableList.of("files"));
        public static final ColumnIdent RECOVERY_FILES_USED =
            new ColumnIdent("recovery", ImmutableList.of("files", "used"));
        public static final ColumnIdent RECOVERY_FILES_REUSED =
            new ColumnIdent("recovery", ImmutableList.of("files", "reused"));
        public static final ColumnIdent RECOVERY_FILES_RECOVERED =
            new ColumnIdent("recovery", ImmutableList.of("files", "recovered"));
        public static final ColumnIdent RECOVERY_FILES_PERCENT =
            new ColumnIdent("recovery", ImmutableList.of("files", "percent"));

        public static final ColumnIdent RECOVERY_SIZE =
            new ColumnIdent("recovery", ImmutableList.of("size"));
        public static final ColumnIdent RECOVERY_SIZE_USED =
            new ColumnIdent("recovery", ImmutableList.of("size", "used"));
        public static final ColumnIdent RECOVERY_SIZE_REUSED =
            new ColumnIdent("recovery", ImmutableList.of("size", "reused"));
        public static final ColumnIdent RECOVERY_SIZE_RECOVERED =
            new ColumnIdent("recovery", ImmutableList.of("size", "recovered"));
        public static final ColumnIdent RECOVERY_SIZE_PERCENT =
            new ColumnIdent("recovery", ImmutableList.of("size", "percent"));

        public static final ColumnIdent PATH = new ColumnIdent("path");
        public static final ColumnIdent BLOB_PATH = new ColumnIdent("blob_path");

        public static final ColumnIdent MIN_LUCENE_VERSION = new ColumnIdent("min_lucene_version");
    }

    public static class ReferenceIdents {

        /**
         * Implementations have to be registered in
         *  - {@link io.crate.metadata.shard.ShardReferenceResolver}
         *  - {@link io.crate.metadata.shard.blob.BlobShardReferenceResolver}
         *  - {@link io.crate.operation.reference.sys.shard.unassigned.UnassignedShardsExpressionFactories}
         */

        public static final ReferenceIdent ID = new ReferenceIdent(IDENT, Columns.ID);
        public static final ReferenceIdent SCHEMA_NAME = new ReferenceIdent(IDENT, Columns.SCHEMA_NAME);
        public static final ReferenceIdent TABLE_NAME = new ReferenceIdent(IDENT, Columns.TABLE_NAME);
        public static final ReferenceIdent PARTITION_IDENT = new ReferenceIdent(IDENT, Columns.PARTITION_IDENT);
        public static final ReferenceIdent NUM_DOCS = new ReferenceIdent(IDENT, Columns.NUM_DOCS);
        public static final ReferenceIdent PRIMARY = new ReferenceIdent(IDENT, Columns.PRIMARY);
        public static final ReferenceIdent RELOCATING_NODE = new ReferenceIdent(IDENT, Columns.RELOCATING_NODE);
        public static final ReferenceIdent SIZE = new ReferenceIdent(IDENT, Columns.SIZE);
        public static final ReferenceIdent STATE = new ReferenceIdent(IDENT, Columns.STATE);
        public static final ReferenceIdent ROUTING_STATE = new ReferenceIdent(IDENT, Columns.ROUTING_STATE);
        public static final ReferenceIdent ORPHAN_PARTITION = new ReferenceIdent(IDENT, Columns.ORPHAN_PARTITION);
        public static final ReferenceIdent RECOVERY = new ReferenceIdent(IDENT, Columns.RECOVERY);
        public static final ReferenceIdent PATH = new ReferenceIdent(IDENT, Columns.PATH);
        public static final ReferenceIdent BLOB_PATH = new ReferenceIdent(IDENT, Columns.BLOB_PATH);
        public static final ReferenceIdent MIN_LUCENE_VERSION = new ReferenceIdent(IDENT, Columns.MIN_LUCENE_VERSION);
    }

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(
        Columns.SCHEMA_NAME,
        Columns.TABLE_NAME,
        Columns.ID,
        Columns.PARTITION_IDENT
    );

    private final TableColumn nodesTableColumn;

    @Inject
    public SysShardsTableInfo(ClusterService service, SysNodesTableInfo sysNodesTableInfo) {
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

                .register(Columns.RECOVERY, ObjectType.INSTANCE)
                .register(Columns.RECOVERY_STAGE, StringType.INSTANCE)
                .register(Columns.RECOVERY_TYPE, StringType.INSTANCE)
                .register(Columns.RECOVERY_TOTAL_TIME, LongType.INSTANCE)

                .register(Columns.RECOVERY_SIZE, ObjectType.INSTANCE)
                .register(Columns.RECOVERY_SIZE_USED, LongType.INSTANCE)
                .register(Columns.RECOVERY_SIZE_REUSED, LongType.INSTANCE)
                .register(Columns.RECOVERY_SIZE_RECOVERED, LongType.INSTANCE)
                .register(Columns.RECOVERY_SIZE_PERCENT, FloatType.INSTANCE)

                .register(Columns.RECOVERY_FILES, ObjectType.INSTANCE)
                .register(Columns.RECOVERY_FILES_USED, IntegerType.INSTANCE)
                .register(Columns.RECOVERY_FILES_REUSED, IntegerType.INSTANCE)
                .register(Columns.RECOVERY_FILES_RECOVERED, IntegerType.INSTANCE)
                .register(Columns.RECOVERY_FILES_PERCENT, FloatType.INSTANCE)
                .register(Columns.PATH, DataTypes.STRING)
                .register(Columns.BLOB_PATH, DataTypes.STRING)

                .register(Columns.MIN_LUCENE_VERSION, StringType.INSTANCE)
                .putInfoOnly(SysNodesTableInfo.SYS_COL_IDENT, SysNodesTableInfo.tableColumnInfo(IDENT)),
            PRIMARY_KEY);
        this.service = service;
        nodesTableColumn = sysNodesTableInfo.tableColumn();
    }

    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        Reference info = super.getReference(columnIdent);
        if (info == null) {
            return nodesTableColumn.getReference(this.ident(), columnIdent);
        }
        return info;
    }

    private void processShardRouting(Map<String, Map<String, List<Integer>>> routing, ShardRouting shardRouting, ShardId shardId) {
        String node;
        int id;
        String index = shardId.getIndex().getName();

        if (shardRouting == null) {
            node = service.localNode().getId();
            id = UnassignedShard.markUnassigned(shardId.id());
        } else {
            node = shardRouting.currentNodeId();
            id = shardRouting.id();
        }
        Map<String, List<Integer>> nodeMap = routing.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            routing.put(node, nodeMap);
        }

        List<Integer> shards = nodeMap.get(index);
        if (shards == null) {
            shards = new ArrayList<>();
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
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        // TODO: filter on whereClause
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        ClusterState state = service.state();
        String[] concreteIndices = state.metaData().getConcreteAllIndices();
        GroupShardsIterator groupShardsIterator = state.getRoutingTable().allAssignedShardsGrouped(concreteIndices, true, true);
        for (final ShardIterator shardIt : groupShardsIterator) {
            final ShardRouting shardRouting = shardIt.nextOrNull();
            processShardRouting(locations, shardRouting, shardIt.shardId());
        }
        return new Routing(locations);
    }
}

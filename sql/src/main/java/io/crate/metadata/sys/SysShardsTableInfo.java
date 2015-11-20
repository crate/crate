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
import io.crate.types.BooleanType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.StringType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;

@Singleton
public class SysShardsTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "shards");

    private final Map<ColumnIdent, ReferenceInfo> infos;
    private final Set<ReferenceInfo> columns;

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
        public static final ColumnIdent ORPHAN_PARTITION = new ColumnIdent("orphan_partition");
    }

    public static class ReferenceIdents {
        public static final ReferenceIdent ID = new ReferenceIdent(IDENT, Columns.ID);
        public static final ReferenceIdent SCHEMA_NAME = new ReferenceIdent(IDENT, Columns.SCHEMA_NAME);
        public static final ReferenceIdent TABLE_NAME = new ReferenceIdent(IDENT, Columns.TABLE_NAME);
        public static final ReferenceIdent PARTITION_IDENT = new ReferenceIdent(IDENT, Columns.PARTITION_IDENT);
        public static final ReferenceIdent NUM_DOCS = new ReferenceIdent(IDENT, Columns.NUM_DOCS);
        public static final ReferenceIdent PRIMARY = new ReferenceIdent(IDENT, Columns.PRIMARY);
        public static final ReferenceIdent RELOCATING_NODE = new ReferenceIdent(IDENT, Columns.RELOCATING_NODE);
        public static final ReferenceIdent SIZE = new ReferenceIdent(IDENT, Columns.SIZE);
        public static final ReferenceIdent STATE = new ReferenceIdent(IDENT, Columns.STATE);
        public static final ReferenceIdent ORPHAN_PARTITION = new ReferenceIdent(IDENT, Columns.ORPHAN_PARTITION);
    }

    private static final ImmutableList<ColumnIdent> primaryKey = ImmutableList.of(
            Columns.SCHEMA_NAME,
            Columns.TABLE_NAME,
            Columns.ID,
            Columns.PARTITION_IDENT
    );

    private final TableColumn nodesTableColumn;

    @Inject
    public SysShardsTableInfo(ClusterService service, SysNodesTableInfo sysNodesTableInfo) {
        super(service);
        nodesTableColumn = sysNodesTableInfo.tableColumn();

        ColumnRegistrar registrar = new ColumnRegistrar(IDENT, RowGranularity.SHARD)
            .register(Columns.SCHEMA_NAME, StringType.INSTANCE)
            .register(Columns.TABLE_NAME, StringType.INSTANCE)
            .register(Columns.ID, IntegerType.INSTANCE)
            .register(Columns.PARTITION_IDENT, StringType.INSTANCE)
            .register(Columns.NUM_DOCS, LongType.INSTANCE)
            .register(Columns.PRIMARY, BooleanType.INSTANCE)
            .register(Columns.RELOCATING_NODE, StringType.INSTANCE)
            .register(Columns.SIZE, LongType.INSTANCE)
            .register(Columns.STATE, StringType.INSTANCE)
            .register(Columns.ORPHAN_PARTITION, BooleanType.INSTANCE)
            .putInfoOnly(SysNodesTableInfo.SYS_COL_IDENT, SysNodesTableInfo.tableColumnInfo(IDENT));
        infos = registrar.infos();
        columns = registrar.columns();
    }

    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        ReferenceInfo info = infos.get(columnIdent);
        if (info == null) {
            return nodesTableColumn.getReferenceInfo(this.ident(), columnIdent);
        }
        return info;
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    private void processShardRouting(Map<String, Map<String, List<Integer>>> routing, ShardRouting shardRouting, ShardId shardId) {
        String node;
        if (shardRouting == null) {
            throw new NoShardAvailableActionException(shardId);
        }

        node = shardRouting.currentNodeId();
        int id = shardRouting.id();
        if (!shardRouting.active()) {
            node = clusterService.localNode().id();
            id = UnassignedShard.markUnassigned(id);
        }
        Map<String, List<Integer>> nodeMap = routing.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            routing.put(node, nodeMap);
        }

        List<Integer> shards = nodeMap.get(shardRouting.getIndex());
        if (shards == null) {
            shards = new ArrayList<>();
            nodeMap.put(shardRouting.getIndex(), shards);
        }
        shards.add(id);
    }


    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.SHARD;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }


    /**
     * Retrieves the routing for sys.shards
     *
     * This routing contains ALL shards of ALL indices.
     * Any shards that are not yet assigned to a node will have a NEGATIVE shard id (see {@link UnassignedShard}
     */
    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        // TODO: filter on whereClause
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        for (ShardRouting shardRouting : clusterService.state().routingTable().allShards()) {
            processShardRouting(locations, shardRouting, null);
        }
        return new Routing(locations);
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return infos.values().iterator();
    }
}

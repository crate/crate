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
import io.crate.planner.RowGranularity;
import io.crate.types.*;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;

public class SysShardsTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "shards");
    private static final String[] CONCRETE_INDICES = new String[]{IDENT.esName()};

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>(7);
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>(7);

    private static final ImmutableList<ColumnIdent> primaryKey = ImmutableList.of(
            new ColumnIdent("schema_name"),
            new ColumnIdent("table_name"),
            new ColumnIdent("id"),
            new ColumnIdent("partition_ident"));

    static {
        register(primaryKey.get(0).fqn(), StringType.INSTANCE, null);
        register(primaryKey.get(1).fqn(), StringType.INSTANCE, null);
        register(primaryKey.get(2).fqn(), IntegerType.INSTANCE, null);
        register(primaryKey.get(3).fqn(), StringType.INSTANCE, null);
        register("num_docs", LongType.INSTANCE, null);
        register("primary", BooleanType.INSTANCE, null);
        register("relocating_node", StringType.INSTANCE, null);
        register("size", LongType.INSTANCE, null);
        register("state", StringType.INSTANCE, null);
        register("orphan_partition", BooleanType.INSTANCE, null);

        INFOS.put(SysNodesTableInfo.SYS_COL_IDENT, SysNodesTableInfo.tableColumnInfo(IDENT));
    }

    private final TableColumn nodesTableColumn;

    public SysShardsTableInfo(ClusterService service, SysSchemaInfo sysSchemaInfo, SysNodesTableInfo sysNodesTableInfo) {
        super(service, sysSchemaInfo);
        nodesTableColumn = sysNodesTableInfo.tableColumn();
    }

    private static ReferenceInfo register(String column, DataType type, List<String> path) {
        ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(IDENT, column, path), RowGranularity.SHARD, type);
        if (info.ident().isColumn()) {
            columns.add(info);
        }
        INFOS.put(info.ident().columnIdent(), info);
        return info;
    }

    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        ReferenceInfo info = INFOS.get(columnIdent);
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
        if (!shardRouting.active()) {
            node = NULL_NODE_ID;
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
        shards.add(shardRouting.id());
    }


    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.SHARD;
    }

    @Override
    public TableIdent ident() {
        return IDENT;
    }


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
    public String[] concreteIndices() {
        return CONCRETE_INDICES;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}

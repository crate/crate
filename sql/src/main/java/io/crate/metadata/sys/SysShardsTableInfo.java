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
import io.crate.DataType;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

public class SysShardsTableInfo extends SysTableInfo {

    public static final TableIdent IDENT = new TableIdent(SCHEMA, "shards");
    private static final String[] PARTITIONS = new String[]{IDENT.name()};

    public static Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>(7);
    private static final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>(7);

    private static final ImmutableList<String> primaryKey = ImmutableList.of("table_name", "id");

    static {
        register(primaryKey.get(0), DataType.STRING, null);
        register(primaryKey.get(1), DataType.INTEGER, null);
        register("num_docs", DataType.LONG, null);
        register("primary", DataType.BOOLEAN, null);
        register("relocating_node", DataType.STRING, null);
        register("size", DataType.LONG, null);
        register("state", DataType.STRING, null);
    }

    private final ClusterService clusterService;

    @Inject
    public SysShardsTableInfo(ClusterService service) {
        clusterService = service;
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
    public ReferenceInfo getColumnInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    private void processShardRouting(Map<String, Map<String, Set<Integer>>> routing, ShardRouting shardRouting, ShardId shardId) {
        String node;
        if (shardRouting == null) {
            throw new NoShardAvailableActionException(shardId);
        }

        node = shardRouting.currentNodeId();

        Map<String, Set<Integer>> nodeMap = routing.get(node);
        if (nodeMap == null) {
            nodeMap = new HashMap<>();
            routing.put(shardRouting.currentNodeId(), nodeMap);
        }

        Set<Integer> shards = nodeMap.get(shardRouting.getIndex());
        if (shards == null) {
            shards = new HashSet<>();
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
    public Routing getRouting(WhereClause whereClause) {
        // TODO: filter on whereClause
        Map<String, Map<String, Set<Integer>>> locations = new HashMap<>();
        for (ShardRouting shardRouting : clusterService.state().routingTable().allShards()) {
            processShardRouting(locations, shardRouting, null);
        }
        return new Routing(locations);
    }

    @Override
    public List<String> primaryKey() {
        return primaryKey;
    }

    @Override
    public String[] partitions() {
        return PARTITIONS;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return INFOS.values().iterator();
    }
}

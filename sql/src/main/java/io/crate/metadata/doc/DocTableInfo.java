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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.*;

public class DocTableInfo implements TableInfo {

    private final ImmutableList<ReferenceInfo> columns;
    private final ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private final TableIdent ident;
    private final List<String> primaryKeys;
    private final String clusteredBy;
    private final String[] concreteIndices;
    private final ClusterService clusterService;

    private final String[] indices;

    private final boolean isAlias;

    public DocTableInfo(TableIdent ident,
                        ImmutableList<ReferenceInfo> columns,
                        ImmutableMap<ColumnIdent, ReferenceInfo> references,
                        List<String> primaryKeys,
                        String clusteredBy,
                        boolean isAlias,
                        String[] concreteIndices,
                        ClusterService clusterService) {
        this.clusterService = clusterService;
        this.columns = columns;
        this.references = references;
        this.ident = ident;
        this.primaryKeys = primaryKeys;
        this.clusteredBy = clusteredBy;
        this.concreteIndices = concreteIndices;
        indices = new String[]{ident.name()};
        this.isAlias = isAlias;
    }

    @Override
    public ReferenceInfo getColumnInfo(ColumnIdent columnIdent) {
        return references.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    private void processShardRouting(Map<String, Map<String, Set<Integer>>> locations, ShardRouting shardRouting, ShardId shardId) {
        String node;
        if (shardRouting == null) {
            throw new NoShardAvailableActionException(shardId);
        }
        node = shardRouting.currentNodeId();
        Map<String, Set<Integer>> nodeMap = locations.get(node);
        if (nodeMap == null) {
            nodeMap = new HashMap<>();
            locations.put(shardRouting.currentNodeId(), nodeMap);
        }

        Set<Integer> shards = nodeMap.get(shardRouting.getIndex());
        if (shards == null) {
            shards = new HashSet<>();
            nodeMap.put(shardRouting.getIndex(), shards);
        }
        shards.add(shardRouting.id());
    }


    @Override
    public Routing getRouting(Function whereClause) {

        Map<String, Map<String, Set<Integer>>> locations = new HashMap<>();
        GroupShardsIterator shardIterators = clusterService.operationRouting().searchShards(
                clusterService.state(),
                indices,
                concreteIndices,
                null, // TODO: compute routing from whereClause
                null // preference
        );
        ShardRouting shardRouting;
        for (ShardIterator shardIterator : shardIterators.iterators()) {
            shardRouting = shardIterator.firstOrNull();
            processShardRouting(locations, shardRouting, shardIterator.shardId());
        }

        return new Routing(locations);
    }

    Map<ColumnIdent, ReferenceInfo> references() {
        return references;
    }

    public List<String> primaryKey() {
        return primaryKeys;
    }

    public String clusteredBy() {
        return clusteredBy;
    }

    @Override
    public boolean isAlias() {
        return isAlias;
    }

    @Override
    public String[] partitions() {
        return concreteIndices;
    }
}

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata.blob;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.AlterBlobTableParameterInfo;
import io.crate.analyze.TableParameterInfo;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.DynamicReference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;

public class BlobTableInfo implements TableInfo {

    private final BlobSchemaInfo blobSchemaInfo;
    private final TableIdent ident;
    private final int numberOfShards;
    private final BytesRef numberOfReplicas;
    private final ClusterService clusterService;
    private final String index;
    private final LinkedHashSet<ReferenceInfo> columns = new LinkedHashSet<>();
    private final BytesRef blobsPath;
    private final TableParameterInfo tableParameterInfo;
    private ImmutableMap<String,Object> tableParameters;

    public static final Map<ColumnIdent, ReferenceInfo> INFOS = new LinkedHashMap<>();

    private static final ImmutableList<ColumnIdent> primaryKey = ImmutableList.of(
            new ColumnIdent("digest"));
    private final static List<Tuple<String, DataType>> staticColumns = ImmutableList.<Tuple<String,DataType>>builder()
                .add(new Tuple<String, DataType>("digest", DataTypes.STRING))
                .add(new Tuple<String, DataType>("last_modified", DataTypes.TIMESTAMP))
                .build();

    public BlobTableInfo(BlobSchemaInfo blobSchemaInfo,
                         TableIdent ident,
                         String index,
                         ClusterService clusterService,
                         int numberOfShards,
                         BytesRef numberOfReplicas,
                         ImmutableMap<String, Object> tableParameters,
                         BytesRef blobsPath) {
        this.blobSchemaInfo = blobSchemaInfo;
        this.ident = ident;
        this.index = index;
        this.clusterService = clusterService;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.blobsPath = blobsPath;
        this.tableParameterInfo = new AlterBlobTableParameterInfo();
        this.tableParameters = tableParameters;

        registerStaticColumns();
    }

    @Override
    public SchemaInfo schemaInfo() {
        return blobSchemaInfo;
    }

    @Nullable
    @Override
    public ReferenceInfo getReferenceInfo(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<ReferenceInfo> columns() {
        return columns;
    }

    @Override
    public List<ReferenceInfo> partitionedByColumns() {
        return ImmutableList.of();
    }

    @Override
    public IndexReferenceInfo indexColumn(ColumnIdent ident) {
        return null;
    }

    @Override
    public RowGranularity rowGranularity() {
        return RowGranularity.DOC;
    }

    @Override
    public TableIdent ident() {
        return ident;
    }

    private void processShardRouting(Map<String, Map<String, List<Integer>>> locations, ShardRouting shardRouting, ShardId shardId) {
        String node;
        if (shardRouting == null) {
            throw new NoShardAvailableActionException(shardId);
        }
        node = shardRouting.currentNodeId();
        Map<String, List<Integer>> nodeMap = locations.get(node);
        if (nodeMap == null) {
            nodeMap = new TreeMap<>();
            locations.put(shardRouting.currentNodeId(), nodeMap);
        }

        List<Integer> shards = nodeMap.get(shardRouting.getIndex());
        if (shards == null) {
            shards = new ArrayList<>();
            nodeMap.put(shardRouting.getIndex(), shards);
        }
        shards.add(shardRouting.id());
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        GroupShardsIterator shardIterators = clusterService.operationRouting().searchShards(
                clusterService.state(),
                Strings.EMPTY_ARRAY,
                new String[]{index},
                null,
                preference
        );
        ShardRouting shardRouting;
        for (ShardIterator shardIterator : shardIterators) {
            shardRouting = shardIterator.nextOrNull();
            processShardRouting(locations, shardRouting, shardIterator.shardId());
        }

        return new Routing(locations);
    }

    @Override
    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public BytesRef numberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public boolean hasAutoGeneratedPrimaryKey() {
        return false;
    }

    @Nullable
    @Override
    public ColumnIdent clusteredBy() {
        return primaryKey.get(0);
    }

    @Override
    public boolean isAlias() {
        return false;
    }

    @Override
    public String[] concreteIndices() {
        return Strings.EMPTY_ARRAY;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    @Override
    public List<PartitionName> partitions() {
        return ImmutableList.of();
    }

    @Override
    public List<ColumnIdent> partitionedBy() {
        return ImmutableList.of();
    }

    @Nullable
    @Override
    public DynamicReference getDynamic(ColumnIdent ident, boolean forWrite) {
        return null;
    }

    @Override
    public ColumnPolicy columnPolicy() {
        return ColumnPolicy.STRICT;
    }

    public DynamicReference getDynamic(ColumnIdent ident) {
        return null;
    }

    @Override
    public Iterator<ReferenceInfo> iterator() {
        return columns.iterator();
    }

    private void registerStaticColumns() {
        for (Tuple<String, DataType> column : staticColumns) {
            ReferenceInfo info = new ReferenceInfo(new ReferenceIdent(ident(), column.v1(), null),
                    RowGranularity.DOC, column.v2());
            if (info.ident().isColumn()) {
                columns.add(info);
            }
            INFOS.put(info.ident().columnIdent(), info);
        }
    }

    public BytesRef blobsPath() {
        return blobsPath;
    }

    @Override
    public TableParameterInfo tableParameterInfo() {
        return tableParameterInfo;
    }

    @Override
    public ImmutableMap<String, Object> tableParameters() {
        return tableParameters;
    }
}

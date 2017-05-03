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
import io.crate.Version;
import io.crate.analyze.AlterBlobTableParameterInfo;
import io.crate.analyze.TableParameterInfo;
import io.crate.analyze.WhereClause;
import io.crate.metadata.*;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.ShardedTable;
import io.crate.metadata.table.StoredTable;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;

public class BlobTableInfo implements TableInfo, ShardedTable, StoredTable {

    private final TableIdent ident;
    private final int numberOfShards;
    private final BytesRef numberOfReplicas;
    private final ClusterService clusterService;
    private final String index;
    private final LinkedHashSet<Reference> columns = new LinkedHashSet<>();
    private final BytesRef blobsPath;
    private final TableParameterInfo tableParameterInfo;
    private final Map<String, Object> tableParameters;
    private final String routingHashFunction;
    private final Version versionCreated;
    private final Version versionUpgraded;
    private final boolean closed;

    private static final Map<ColumnIdent, Reference> INFOS = new LinkedHashMap<>();

    private static final ImmutableList<ColumnIdent> PRIMARY_KEY = ImmutableList.of(new ColumnIdent("digest"));
    private final static List<Tuple<String, DataType>> STATIC_COLUMNS = ImmutableList.<Tuple<String, DataType>>builder()
        .add(new Tuple<>("digest", DataTypes.STRING))
        .add(new Tuple<>("last_modified", DataTypes.TIMESTAMP))
        .build();

    public BlobTableInfo(TableIdent ident,
                         String index,
                         ClusterService clusterService,
                         int numberOfShards,
                         BytesRef numberOfReplicas,
                         Map<String, Object> tableParameters,
                         BytesRef blobsPath,
                         String routingHashFunction,
                         @Nullable Version versionCreated,
                         @Nullable Version versionUpgraded,
                         boolean closed) {
        this.ident = ident;
        this.index = index;
        this.clusterService = clusterService;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.blobsPath = blobsPath;
        this.tableParameterInfo = new AlterBlobTableParameterInfo();
        this.tableParameters = tableParameters;
        this.routingHashFunction = routingHashFunction;
        this.versionCreated = versionCreated;
        this.versionUpgraded = versionUpgraded;
        this.closed = closed;

        registerStaticColumns();
    }

    @Nullable
    @Override
    public Reference getReference(ColumnIdent columnIdent) {
        return INFOS.get(columnIdent);
    }

    @Override
    public Collection<Reference> columns() {
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

        String indexName = shardRouting.getIndexName();
        List<Integer> shards = nodeMap.get(indexName);
        if (shards == null) {
            shards = new ArrayList<>();
            nodeMap.put(indexName, shards);
        }
        shards.add(shardRouting.id());
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        Map<String, Map<String, List<Integer>>> locations = new TreeMap<>();
        GroupShardsIterator shardIterators = clusterService.operationRouting().searchShards(
            clusterService.state(),
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
        return PRIMARY_KEY;
    }

    @Override
    public int numberOfShards() {
        return numberOfShards;
    }

    @Override
    public BytesRef numberOfReplicas() {
        return numberOfReplicas;
    }

    @Nullable
    @Override
    public ColumnIdent clusteredBy() {
        return PRIMARY_KEY.get(0);
    }

    @Override
    public Iterator<Reference> iterator() {
        return columns.iterator();
    }

    private void registerStaticColumns() {
        for (Tuple<String, DataType> column : STATIC_COLUMNS) {
            Reference ref = new Reference(
                new ReferenceIdent(ident(), column.v1(), null), RowGranularity.DOC, column.v2());
            assert ref.ident().isColumn() : "only top-level columns should be added to columns list";
            columns.add(ref);
            INFOS.put(ref.ident().columnIdent(), ref);
        }
    }

    public BytesRef blobsPath() {
        return blobsPath;
    }

    public TableParameterInfo tableParameterInfo() {
        return tableParameterInfo;
    }

    public Map<String, Object> tableParameters() {
        return tableParameters;
    }

    @Override
    public Set<Operation> supportedOperations() {
        return Operation.READ_ONLY;
    }

    public String concreteIndex() {
        return index;
    }

    @Override
    public String routingHashFunction() {
        return routingHashFunction;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Nullable
    @Override
    public Version versionCreated() {
        return versionCreated;
    }

    @Nullable
    @Override
    public Version versionUpgraded() {
        return versionUpgraded;
    }

}

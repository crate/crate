/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.node.dql;

import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.where.DocKeys;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnavailableShardsException;
import io.crate.executor.transport.task.elasticsearch.ESCommonUtils;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.projection.Projection;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

/**
 * A {@link Collect} phase to retrieve data by primary key from the (primary) shard.
 * Internally, the {@link io.crate.operation.primarykey.PrimaryKeyLookupOperation}
 * uses the {@link org.elasticsearch.index.get.ShardGetService} with the realtime flag
 * to ensure that data is read from the transaction log which contains the latest data,
 * even if it has not been reflected in the Lucene readers.
 */
public class PrimaryKeyLookupPhase extends AbstractProjectionsPhase implements CollectPhase {

    private List<Symbol> toCollect;
    private DistributionInfo distributionInfo;
    private RowGranularity maxRowGranularity = RowGranularity.CLUSTER;
    private Map<ColumnIdent, Integer> pkMapping;
    private Map<String, Map<ShardId, List<DocKeys.DocKey>>> docKeysPerShardNode;
    private TableIdent tableIdent;
    private boolean isPartitioned;
    private transient DocKeys docKeys;

    public PrimaryKeyLookupPhase(
            UUID jobId,
            int executionNodeId,
            String name,
            RowGranularity rowGranularity,
            Map<ColumnIdent, Integer> pkMapping,
            Map<String, Map<ShardId, List<DocKeys.DocKey>>> docKeysPerShardNode,
            TableIdent tableIdent,
            boolean isPartitioned,
            List<Symbol> toCollect,
            List<Projection> projections,
            @Nullable DocKeys docKeys,
            DistributionInfo distributionInfo)
    {
        super(jobId, executionNodeId, name, projections);
        this.docKeysPerShardNode = docKeysPerShardNode;
        this.docKeys = docKeys;
        this.pkMapping = pkMapping;
        this.tableIdent = tableIdent;
        this.isPartitioned = isPartitioned;
        this.toCollect = toCollect;
        this.distributionInfo = distributionInfo;
        this.outputTypes = extractOutputTypes(toCollect, projections);
        this.maxRowGranularity = rowGranularity;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        super.replaceSymbols(replaceFunction);
        for (Map<ShardId, List<DocKeys.DocKey>> shardMap : docKeysPerShardNode.values()) {
            for (List<DocKeys.DocKey> docKeys : shardMap.values()) {
                for (DocKeys.DocKey docKey : docKeys) {
                    Lists2.replaceItems(docKey.values(), replaceFunction);
                }
            }
        }
        Lists2.replaceItems(toCollect, replaceFunction);
    }

    @Override
    public Type type() {
        return Type.PRIMARY_KEY_LOOKUP;
    }

    @Override
    public Set<String> nodeIds() {
        return docKeysPerShardNode.keySet();
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    public DocKeys docKeys() {
        if (docKeys == null) {
            throw new RuntimeException("DocKeys are transient and become null after serialization.");
        }
        return docKeys;
    }

    public Map<ColumnIdent, Integer> pkMapping() {
        return pkMapping;
    }

    public Map<String, Map<ShardId, List<DocKeys.DocKey>>> getDocKeysPerShardNode() {
        return docKeysPerShardNode;
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public RowGranularity maxRowGranularity() {
        return maxRowGranularity;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitPrimaryKeyLookupPhase(this, context);
    }

    public PrimaryKeyLookupPhase(StreamInput in) throws IOException {
        super(in);

        int numPKMappings = in.readVInt();
        pkMapping = new HashMap<>(numPKMappings);
        for (int i = 0; i < numPKMappings; i++) {
            ColumnIdent columnIdent = new ColumnIdent(in);
            int pkPos = in.readVInt();
            pkMapping.put(columnIdent, pkPos);
        }

        int numNodes = in.readVInt();
        docKeysPerShardNode = new HashMap<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            String nodeId = in.readString();
            int numShards = in.readVInt();
            Map<ShardId, List<DocKeys.DocKey>> shardMap = new HashMap<>(numShards);
            for (int j = 0; j < numShards; j++) {
                ShardId shardId = ShardId.readShardId(in);
                int numDocKeys = in.readVInt();
                List<DocKeys.DocKey> docKeyList = new ArrayList<>(numDocKeys);
                for (int k = 0; k < numDocKeys; k++) {
                    DocKeys.DocKey docKey = DocKeys.DocKey.readFrom(in);
                    docKeyList.add(k, docKey);
                }
                shardMap.put(shardId, docKeyList);
            }
            docKeysPerShardNode.put(nodeId, shardMap);
        }

        tableIdent = TableIdent.fromStream(in);
        isPartitioned = in.readBoolean();

        distributionInfo = DistributionInfo.fromStream(in);

        toCollect = Symbols.listFromStream(in);

        maxRowGranularity = RowGranularity.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(pkMapping.size());
        for (Map.Entry<ColumnIdent, Integer> entry : pkMapping.entrySet()) {
            ColumnIdent key = entry.getKey();
            key.writeTo(out);
            Integer value = entry.getValue();
            out.writeVInt(value);
        }

        int numNodes = docKeysPerShardNode.size();
        out.writeVInt(numNodes);
        for (Map.Entry<String, Map<ShardId, List<DocKeys.DocKey>>> nodeEntry : docKeysPerShardNode.entrySet()) {
            out.writeString(nodeEntry.getKey());
            Map<ShardId, List<DocKeys.DocKey>> nodeMap = nodeEntry.getValue();
            int numShards = nodeMap.size();
            out.writeVInt(numShards);
            for (Map.Entry<ShardId, List<DocKeys.DocKey>> shardEntry : nodeMap.entrySet()) {
                ShardId shardId = shardEntry.getKey();
                shardId.writeTo(out);
                List<DocKeys.DocKey> docKeys = shardEntry.getValue();
                int numDocKeys = docKeys.size();
                out.writeVInt(numDocKeys);
                for (DocKeys.DocKey docKey : docKeys) {
                    docKey.writeTo(out);
                }
            }
        }

        tableIdent.writeTo(out);
        out.writeBoolean(isPartitioned);

        distributionInfo.writeTo(out);

        Symbols.toStream(toCollect, out);

        RowGranularity.toStream(maxRowGranularity, out);
    }

    private static Map<ColumnIdent, Integer> retrievePKMapping(
            List<Symbol> toCollect, DocTableInfo docTableInfo)
    {
        Map<ColumnIdent, Integer> result = new HashMap<>();
        for (Symbol symbol : toCollect) {
            if (symbol instanceof Reference) {
                ColumnIdent columnIdent = ((Reference) symbol).ident().columnIdent();
                if (docTableInfo.isPartitioned() && docTableInfo.partitionedBy().contains(columnIdent)) {
                    int pkPos = docTableInfo.primaryKey().indexOf(columnIdent);
                    if (pkPos >= 0) {
                        result.put(columnIdent, pkPos);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Constructs a HashMap which contains a list of DocKeys (=where clause values)
     * grouped by their primary shard ids and node ids. This enables us to quickly
     * perform the ES Get requests on the execution nodes.
     */
    private static Map<String, Map<ShardId, List<DocKeys.DocKey>>> docKeysByShardIdAndNodeId (
            DocKeys docKeys,
            ClusterService clusterService,
            DocTableInfo tableInfo
    ) {

        ClusterState clusterState = clusterService.state();
        OperationRouting routing = clusterService.operationRouting();

        Map<String, Map<ShardId, List<DocKeys.DocKey>>> result = new HashMap<>();

        for (DocKeys.DocKey docKey : docKeys) {
            if (docKey.id() == null) {
                // A primary key is not allowed to be null.
                // Thus, we skip this key here. It cannot match.
                continue;
            }
            String indexName = ESCommonUtils.indexName(
                tableInfo.isPartitioned(),
                tableInfo.ident(),
                docKey.partitionValues().orElse(null));
            final ShardIterator shards;
            try {
                shards = routing.getShards(
                    clusterState,
                    indexName,
                    docKey.id(),
                    docKey.routing(),
                    Preference.PRIMARY.type()
                );
            } catch (IndexNotFoundException e) {
                if (tableInfo.isPartitioned()) {
                    // May seem odd but partitions are mapped to indices
                    // which may not exist yet because they are created
                    // as new keys are inserted.
                    continue;
                }
                throw e;
            }
            ShardId shardId = shards.shardId();
            ShardRouting shardRouting = shards.nextOrNull();
            if (shardRouting == null || !shardRouting.primary() || shardRouting.currentNodeId() == null) {
                if (tableInfo.isPartitioned()) {
                    // Same as above. Shards of partitioned tables
                    // may not be available yet. This is a more
                    // relaxed assumption because we found the
                    // corresponding partition index. Actually, we
                    // should normally expected the shard to be available
                    // but if a lot of partitions are created we
                    // could easily run into this situation.
                    continue;
                }
                throw new UnavailableShardsException(shardId);
            }
            String nodeID = shardRouting.currentNodeId();

            Map<ShardId, List<DocKeys.DocKey>> shardIdMap = result.get(nodeID);
            if (shardIdMap == null) {
                shardIdMap = new HashMap<>(1);
                result.put(nodeID, shardIdMap);
            }
            List<DocKeys.DocKey> docKeyList = shardIdMap.get(shardId);
            if (docKeyList == null) {
                docKeyList = new ArrayList<>(1);
                shardIdMap.put(shardId, docKeyList);
            }
            docKeyList.add(docKey);
        }

        return result;
    }

    public static PrimaryKeyLookupPhase forQueriedTable(
            Planner.Context plannerContext,
            QueriedDocTable table,
            List<Symbol> toCollect,
            List<Projection> projections,
            DocKeys docKeys) {

        DocTableInfo tableInfo = table.tableRelation().tableInfo();

        return new PrimaryKeyLookupPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "primaryKeyLookupPhase",
            tableInfo.rowGranularity(),
            retrievePKMapping(toCollect, tableInfo),
            docKeysByShardIdAndNodeId(docKeys, plannerContext.getClusterService(), tableInfo),
            tableInfo.ident(),
            tableInfo.isPartitioned(),
            toCollect,
            projections,
            docKeys,
            DistributionInfo.DEFAULT_BROADCAST);
    }

}


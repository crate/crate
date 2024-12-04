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

package io.crate.execution.dsl.phases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.operators.PKAndVersion;
import io.crate.types.DataType;

public final class PKLookupPhase extends AbstractProjectionsPhase implements CollectPhase {

    private final List<ColumnIdent> partitionedByColumns;
    private final List<Symbol> toCollect;
    private final Map<String, Map<ShardId, List<PKAndVersion>>> idsByShardByNode;
    private DistributionInfo distInfo = DistributionInfo.DEFAULT_BROADCAST;

    public PKLookupPhase(UUID jobId,
                         int phaseId,
                         List<ColumnIdent> partitionedByColumns,
                         List<Symbol> toCollect,
                         Map<String, Map<ShardId, List<PKAndVersion>>> idsByShardByNode) {
        super(jobId, phaseId, "pkLookup", Collections.emptyList());
        assert toCollect.stream().noneMatch(
            st -> st.any(s -> s instanceof ScopedSymbol || s instanceof SelectSymbol))
            : "toCollect must not contain any fields or selectSymbols: " + toCollect;
        this.partitionedByColumns = partitionedByColumns;
        this.toCollect = toCollect;
        this.idsByShardByNode = idsByShardByNode;
    }

    public PKLookupPhase(StreamInput in) throws IOException {
        super(in);
        distInfo = new DistributionInfo(in);
        toCollect = Symbols.fromStream(in);

        int numNodes = in.readVInt();
        idsByShardByNode = new HashMap<>(numNodes);
        for (int nodeIdx = 0; nodeIdx < numNodes; nodeIdx++) {
            String nodeId = in.readString();
            int numShards = in.readVInt();
            HashMap<ShardId, List<PKAndVersion>> idsByShard = new HashMap<>(numShards);
            idsByShardByNode.put(nodeId, idsByShard);
            for (int shardIdx = 0; shardIdx < numShards; shardIdx++) {
                ShardId shardId = new ShardId(in);
                int numPks = in.readVInt();
                ArrayList<PKAndVersion> pks = new ArrayList<>(numPks);
                for (int pkIdx = 0; pkIdx < numPks; pkIdx++) {
                    pks.add(new PKAndVersion(in));
                }
                idsByShard.put(shardId, pks);
            }
        }

        int numPartitionedByCols = in.readVInt();
        if (numPartitionedByCols == 0) {
            partitionedByColumns = Collections.emptyList();
        } else {
            partitionedByColumns = new ArrayList<>(numPartitionedByCols);
            for (int i = 0; i < numPartitionedByCols; i++) {
                partitionedByColumns.add(ColumnIdent.of(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        distInfo.writeTo(out);
        Symbols.toStream(toCollect, out);

        out.writeVInt(idsByShardByNode.size());
        for (Map.Entry<String, Map<ShardId, List<PKAndVersion>>> byNodeEntry : idsByShardByNode.entrySet()) {
            Map<ShardId, List<PKAndVersion>> idsByShard = byNodeEntry.getValue();
            out.writeString(byNodeEntry.getKey());
            out.writeVInt(idsByShard.size());
            for (Map.Entry<ShardId, List<PKAndVersion>> shardEntry : idsByShard.entrySet()) {
                List<PKAndVersion> ids = shardEntry.getValue();

                shardEntry.getKey().writeTo(out);
                out.writeVInt(ids.size());
                for (PKAndVersion id : ids) {
                    id.writeTo(out);
                }
            }
        }

        out.writeVInt(partitionedByColumns.size());
        for (ColumnIdent partitionedByColumn : partitionedByColumns) {
            partitionedByColumn.writeTo(out);
        }
    }

    @Override
    public List<DataType<?>> outputTypes() {
        if (projections.isEmpty()) {
            return Symbols.typeView(toCollect);
        }
        return super.outputTypes();
    }

    @Override
    public List<Symbol> toCollect() {
        return toCollect;
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distInfo = distributionInfo;
    }

    @Override
    public Type type() {
        return Type.PKLookup;
    }

    @Override
    public Collection<String> nodeIds() {
        return idsByShardByNode.keySet();
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitPKLookup(this, context);
    }

    public Map<ShardId,List<PKAndVersion>> getIdsByShardId(String nodeId) {
        return idsByShardByNode.getOrDefault(nodeId, Collections.emptyMap());
    }

    public List<ColumnIdent> partitionedByColumns() {
        return partitionedByColumns;
    }
}


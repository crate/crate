/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.projection;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FetchProjection extends Projection {

    public static final ProjectionFactory<FetchProjection> FACTORY = new ProjectionFactory<FetchProjection>() {
        @Override
        public FetchProjection newInstance() {
            return new FetchProjection();
        }
    };

    private IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId;
    private Symbol docIdSymbol;
    private List<Symbol> inputSymbols;
    private List<Symbol> outputSymbols;
    private List<ReferenceInfo> partitionBy;
    private Map<Integer, List<String>> executionNodes;
    private int bulkSize;
    private boolean closeContexts;
    private IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard;

    private FetchProjection() {
    }

    public FetchProjection(IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId,
                           Symbol docIdSymbol,
                           List<Symbol> inputSymbols,
                           List<Symbol> outputSymbols,
                           List<ReferenceInfo> partitionBy,
                           Map<Integer, List<String>> executionNodes,
                           int bulkSize,
                           boolean closeContexts,
                           IntObjectOpenHashMap<String> jobSearchContextIdToNode,
                           IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard) {
        this.jobSearchContextIdToExecutionNodeId = jobSearchContextIdToExecutionNodeId;
        this.docIdSymbol = docIdSymbol;
        this.inputSymbols = inputSymbols;
        this.outputSymbols = outputSymbols;
        this.partitionBy = partitionBy;
        this.executionNodes = executionNodes;
        this.bulkSize = bulkSize;
        this.closeContexts = closeContexts;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.jobSearchContextIdToShard = jobSearchContextIdToShard;
    }

    public IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId() {
        return jobSearchContextIdToExecutionNodeId;
    }

    public Symbol docIdSymbol() {
        return docIdSymbol;
    }

    public List<Symbol> inputSymbols() {
        return inputSymbols;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public List<ReferenceInfo> partitionedBy() {
        return partitionBy;
    }

    public Map<Integer, List<String>> executionNodes() {
        return executionNodes;
    }

    public int bulkSize() {
        return bulkSize;
    }

    public boolean closeContexts() {
        return closeContexts;
    }


    public IntObjectOpenHashMap<String> jobSearchContextIdToNode() {
        return jobSearchContextIdToNode;
    }

    public IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard() {
        return jobSearchContextIdToShard;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FETCH;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFetchProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchProjection that = (FetchProjection) o;

        if (closeContexts != that.closeContexts) return false;
        if (bulkSize != that.bulkSize) return false;
        if (!jobSearchContextIdToExecutionNodeId.equals(that.jobSearchContextIdToExecutionNodeId)) return false;
        if (!executionNodes.equals(that.executionNodes)) return false;
        if (!docIdSymbol.equals(that.docIdSymbol)) return false;
        if (!inputSymbols.equals(that.inputSymbols)) return false;
        if (!outputSymbols.equals(that.outputSymbols)) return false;
        if (!outputSymbols.equals(that.outputSymbols)) return false;
        return partitionBy.equals(that.partitionBy);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + jobSearchContextIdToExecutionNodeId.hashCode();
        result = 31 * result + docIdSymbol.hashCode();
        result = 31 * result + inputSymbols.hashCode();
        result = 31 * result + outputSymbols.hashCode();
        result = 31 * result + partitionBy.hashCode();
        result = 31 * result + executionNodes.hashCode();
        result = 31 * result + bulkSize;
        result = 31 * result + (closeContexts ? 1 : 0);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        docIdSymbol = Symbol.fromStream(in);
        int inputSymbolsSize = in.readVInt();
        inputSymbols = new ArrayList<>(inputSymbolsSize);
        for (int i = 0; i < inputSymbolsSize; i++) {
            inputSymbols.add(Symbol.fromStream(in));
        }
        int outputSymbolsSize = in.readVInt();
        outputSymbols = new ArrayList<>(outputSymbolsSize);
        for (int i = 0; i < outputSymbolsSize; i++) {
            outputSymbols.add(Symbol.fromStream(in));
        }
        int partitionedBySize = in.readVInt();
        partitionBy = new ArrayList<>(partitionedBySize);
        for (int i = 0; i < partitionedBySize; i++) {
            ReferenceInfo referenceInfo = new ReferenceInfo();
            referenceInfo.readFrom(in);
            partitionBy.add(referenceInfo);
        }

        int collectNodesSize = in.readVInt();
        executionNodes = new HashMap<>(collectNodesSize);
        for (int i = 0; i < collectNodesSize; i++) {
            Integer executionNodeId = in.readVInt();
            int executionNodesSize = in.readVInt();
            List<String> nodes = new ArrayList<>(executionNodesSize);
            for (int j = 0; j < executionNodesSize; i++) {
                nodes.add(in.readString());
            }
            executionNodes.put(executionNodeId, nodes);
        }


        bulkSize = in.readVInt();
        closeContexts = in.readBoolean();

        int numJobSearchContextIdToExecutionNodeId = in.readVInt();
        jobSearchContextIdToExecutionNodeId = new IntObjectOpenHashMap<>(numJobSearchContextIdToExecutionNodeId);
        for (int i = 0; i < numJobSearchContextIdToExecutionNodeId; i++) {
            jobSearchContextIdToNode.put(in.readVInt(), in.readString());
        }
        int numJobSearchContextIdToNode = in.readVInt();
        jobSearchContextIdToNode = new IntObjectOpenHashMap<>(numJobSearchContextIdToNode);
        for (int i = 0; i < numJobSearchContextIdToNode; i++) {
            jobSearchContextIdToNode.put(in.readVInt(), in.readString());
        }
        int numJobSearchContextIdToShard = in.readVInt();
        jobSearchContextIdToShard = new IntObjectOpenHashMap<>(numJobSearchContextIdToShard);
        for (int i = 0; i < numJobSearchContextIdToShard; i++) {
            jobSearchContextIdToShard.put(in.readVInt(), ShardId.readShardId(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(docIdSymbol, out);
        out.writeVInt(inputSymbols.size());
        for (Symbol symbol : inputSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeVInt(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            Symbol.toStream(symbol, out);
        }
        out.writeVInt(partitionBy.size());
        for (ReferenceInfo referenceInfo : partitionBy) {
            referenceInfo.writeTo(out);
        }

        out.writeVInt(executionNodes.size());
        for (Map.Entry<Integer, List<String>> executionNode : executionNodes.entrySet()) {
            out.writeVInt(executionNode.getKey());
            List<String> nodes = executionNode.getValue();
            out.writeVInt(nodes.size());
            for (String nodeId : nodes) {
                out.writeString(nodeId);
            }
        }
        out.writeVInt(bulkSize);
        out.writeBoolean(closeContexts);

        out.writeVInt(jobSearchContextIdToExecutionNodeId.size());
        for (IntObjectCursor<Integer> entry : jobSearchContextIdToExecutionNodeId) {
            out.writeVInt(entry.key);
            out.writeVInt(entry.value);
        }
        out.writeVInt(jobSearchContextIdToNode.size());
        for (IntObjectCursor<String> entry : jobSearchContextIdToNode) {
            out.writeVInt(entry.key);
            out.writeString(entry.value);
        }
        out.writeVInt(jobSearchContextIdToShard.size());
        for (IntObjectCursor<ShardId> entry : jobSearchContextIdToShard) {
            out.writeVInt(entry.key);
            entry.value.writeTo(out);
        }
    }
}

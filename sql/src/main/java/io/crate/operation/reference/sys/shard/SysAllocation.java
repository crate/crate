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

package io.crate.operation.reference.sys.shard;

import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class SysAllocation {

    private final ShardId shardId;
    private final ShardRoutingState currentState;
    private final ClusterAllocationExplanation explanation;
    private final IndexParts indexParts;
    private final ShardAllocationDecision decision;
    private final List<SysAllocationNodeDecision> decisions;

    public SysAllocation(ClusterAllocationExplanation explanation) {
        this.explanation = explanation;
        this.shardId = explanation.getShard();
        this.currentState = explanation.getShardState();
        this.indexParts = new IndexParts(explanation.getShard().getIndexName());
        this.decision = explanation.getShardAllocationDecision();
        this.decisions = nodeDecisions();
    }

    public String tableSchema() {
        return indexParts.getSchema();
    }

    public String tableName() {
        return indexParts.getTable();
    }

    @Nullable
    public String partitionIdent() {
        String indexName = shardId.getIndexName();
        if (IndexParts.isPartitioned(indexName)) {
            return PartitionName.fromIndexOrTemplate(indexName).ident();
        }
        return null;
    }

    public int shardId() {
        return shardId.id();
    }

    @Nullable
    public String nodeId() {
        return explanation.getCurrentNode() == null ? null : explanation.getCurrentNode().getId();
    }

    public boolean primary() {
        return explanation.isPrimary();
    }

    public ShardRoutingState currentState() {
        return currentState;
    }

    public String explanation() {
        if (decision.getMoveDecision().isDecisionTaken()) {
            return decision.getMoveDecision().getExplanation();
        } else if (decision.getAllocateDecision().isDecisionTaken()) {
            return decision.getAllocateDecision().getExplanation();
        }
        return null;
    }

    @Nullable
    public List<SysAllocationNodeDecision> decisions() {
        return decisions.isEmpty() ? null : decisions;
    }

    private List<SysAllocationNodeDecision> nodeDecisions() {
        List<SysAllocationNodeDecision> decisions = new ArrayList<>();
        if (decision.getMoveDecision().isDecisionTaken()) {
            decision.getMoveDecision()
                .getNodeDecisions()
                .forEach(i -> decisions.add(SysAllocationNodeDecision.fromNodeAllocationResult(i)));
        } else if (decision.getAllocateDecision().isDecisionTaken()) {
            decision.getAllocateDecision()
                .getNodeDecisions()
                .forEach(i -> decisions.add(SysAllocationNodeDecision.fromNodeAllocationResult(i)));
        }
        return decisions;
    }

    public String fqn() {
        return indexParts.toFullyQualifiedName();
    }

    public static class SysAllocationNodeDecision {

        final String nodeId;
        final String nodeName;
        final List<String> explanations;

        private SysAllocationNodeDecision(String nodeId, String nodeName, @Nullable List<String> explanations) {
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.explanations = explanations;
        }

        static SysAllocationNodeDecision fromNodeAllocationResult(NodeAllocationResult allocationResult) {
            return new SysAllocationNodeDecision(allocationResult.getNode().getId(),
                allocationResult.getNode().getName(),
                getExplanations(allocationResult));
        }

        @Nullable
        private static List<String> getExplanations(NodeAllocationResult allocationResult) {
            if (allocationResult.getCanAllocateDecision() != null &&
                allocationResult.getCanAllocateDecision().getDecisions().isEmpty() == false) {
                List<String> explanations = new ArrayList<>();
                allocationResult.getCanAllocateDecision()
                    .getDecisions()
                    .forEach(d -> {
                        if (d.getExplanation() != null) {
                            explanations.add(d.getExplanation());
                        }
                    });
                return explanations;
            } else {
                return null;
            }
        }

        public String nodeId() {
            return nodeId;
        }

        public String nodeName() {
            return nodeName;
        }

        @Nullable
        public BytesRef[] explanationsAsBytesRefs() {
            if (explanations == null) {
                return null;
            }
            return explanations.stream()
                .map(BytesRefs::toBytesRef)
                .toArray(BytesRef[]::new);
        }
    }
}

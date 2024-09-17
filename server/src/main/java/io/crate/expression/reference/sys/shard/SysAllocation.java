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

package io.crate.expression.reference.sys.shard;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;

public class SysAllocation {

    private final ShardId shardId;
    private final ShardRoutingState currentState;
    private final IndexParts indexParts;
    private final Supplier<ShardAllocationDecision> computeDecision;
    private final String nodeId;
    private final boolean primary;

    private ShardAllocationDecision decision;
    private List<SysAllocationNodeDecision> decisions;

    public SysAllocation(ShardId shardId,
                         ShardRoutingState routingState,
                         Supplier<ShardAllocationDecision> computeDecision,
                         @Nullable String nodeId,
                         boolean isPrimary) {
        this.shardId = shardId;
        this.currentState = routingState;
        this.indexParts = IndexName.decode(shardId.getIndexName());
        this.computeDecision = computeDecision;
        this.nodeId = nodeId;
        this.primary = isPrimary;
    }

    public String tableSchema() {
        return indexParts.schema();
    }

    public String tableName() {
        return indexParts.table();
    }

    @Nullable
    public String partitionIdent() {
        return indexParts.isPartitioned() ? indexParts.partitionIdent() : null;
    }

    public int shardId() {
        return shardId.id();
    }

    @Nullable
    public String nodeId() {
        return nodeId;
    }

    public boolean primary() {
        return primary;
    }

    public ShardRoutingState currentState() {
        return currentState;
    }

    public String explanation() {
        if (decision == null) {
            decision = computeDecision.get();
        }
        if (decision.getMoveDecision().isDecisionTaken()) {
            return decision.getMoveDecision().getExplanation();
        } else if (decision.getAllocateDecision().isDecisionTaken()) {
            return decision.getAllocateDecision().getExplanation();
        }
        return null;
    }

    @Nullable
    public List<SysAllocationNodeDecision> decisions() {
        if (decision == null) {
            decision = computeDecision.get();
            decisions = nodeDecisions();
        }
        return decisions.isEmpty() ? null : decisions;
    }

    private List<SysAllocationNodeDecision> nodeDecisions() {
        assert decision != null : "decision must be initialized to generate nodeDecisions";
        if (decision.getMoveDecision().isDecisionTaken()) {
            return Lists.map(
                decision.getMoveDecision().getNodeDecisions(), SysAllocationNodeDecision::fromNodeAllocationResult);
        } else if (decision.getAllocateDecision().isDecisionTaken()) {
            return Lists.map(
                decision.getAllocateDecision().getNodeDecisions(), SysAllocationNodeDecision::fromNodeAllocationResult);
        } else {
            return List.of();
        }
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
            return new SysAllocationNodeDecision(
                allocationResult.getNode().getId(),
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
        public List<String> explanations() {
            return explanations;
        }
    }
}

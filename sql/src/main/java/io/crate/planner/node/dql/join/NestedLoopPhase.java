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

package io.crate.planner.node.dql.join;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitors;
import io.crate.analyze.symbol.Symbols;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.dql.AbstractProjectionsPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

public class NestedLoopPhase extends AbstractProjectionsPhase implements UpstreamPhase {

    private final Collection<String> executionNodes;
    private final MergePhase leftMergePhase;
    private final MergePhase rightMergePhase;
    private final JoinType joinType;

    @Nullable
    private final Symbol joinCondition;
    private final int numLeftOutputs;
    private final int numRightOutputs;

    private DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

    public NestedLoopPhase(UUID jobId,
                           int executionNodeId,
                           String name,
                           List<Projection> projections,
                           @Nullable MergePhase leftMergePhase,
                           @Nullable MergePhase rightMergePhase,
                           Collection<String> executionNodes,
                           JoinType joinType,
                           @Nullable Symbol joinCondition,
                           int numLeftOutputs,
                           int numRightOutputs) {
        super(jobId, executionNodeId, name, projections);
        Projection lastProjection = Iterables.getLast(projections, null);
        assert lastProjection != null : "lastProjection must not be null";
        assert joinCondition == null || !SymbolVisitors.any(Symbols.IS_COLUMN, joinCondition)
            : "joinCondition must not contain columns: " +  joinCondition;
        outputTypes = Symbols.typeView(lastProjection.outputs());
        this.leftMergePhase = leftMergePhase;
        this.rightMergePhase = rightMergePhase;
        this.executionNodes = executionNodes;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
        this.numLeftOutputs = numLeftOutputs;
        this.numRightOutputs = numRightOutputs;
    }

    @Override
    public Type type() {
        return Type.NESTED_LOOP;
    }

    @Override
    public Collection<String> nodeIds() {
        if (executionNodes == null) {
            return ImmutableSet.of();
        } else {
            return executionNodes;
        }
    }

    @Nullable
    public MergePhase leftMergePhase() {
        return leftMergePhase;
    }

    @Nullable
    public MergePhase rightMergePhase() {
        return rightMergePhase;
    }

    @Nullable
    public Symbol joinCondition() {
        return joinCondition;
    }

    public JoinType joinType() {
        return joinType;
    }

    public int numLeftOutputs() {
        return numLeftOutputs;
    }

    public int numRightOutputs() {
        return numRightOutputs;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopPhase(this, context);
    }


    public NestedLoopPhase(StreamInput in) throws IOException {
        super(in);

        distributionInfo = DistributionInfo.fromStream(in);

        int numExecutionNodes = in.readVInt();
        if (numExecutionNodes > 0) {
            executionNodes = new HashSet<>(numExecutionNodes);
            for (int i = 0; i < numExecutionNodes; i++) {
                executionNodes.add(in.readString());
            }
        } else {
            executionNodes = null;
        }
        leftMergePhase = in.readOptionalWriteable(MergePhase::new);
        rightMergePhase = in.readOptionalWriteable(MergePhase::new);
        if (in.readBoolean()) {
            joinCondition = Symbols.fromStream(in);
        } else {
            joinCondition = null;
        }
        joinType = JoinType.values()[in.readVInt()];
        numLeftOutputs = in.readVInt();
        numRightOutputs = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        distributionInfo.writeTo(out);

        if (executionNodes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(executionNodes.size());
            for (String node : executionNodes) {
                out.writeString(node);
            }
        }

        out.writeOptionalWriteable(leftMergePhase);
        out.writeOptionalWriteable(rightMergePhase);

        if (joinCondition == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            Symbols.toStream(joinCondition, out);
        }

        out.writeVInt(joinType.ordinal());
        out.writeVInt(numLeftOutputs);
        out.writeVInt(numRightOutputs);
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
            .add("executionPhaseId", phaseId())
            .add("name", name())
            .add("joinType", joinType)
            .add("joinCondition", joinCondition)
            .add("outputTypes", outputTypes)
            .add("jobId", jobId())
            .add("executionNodes", executionNodes);
        return helper.toString();
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }
}

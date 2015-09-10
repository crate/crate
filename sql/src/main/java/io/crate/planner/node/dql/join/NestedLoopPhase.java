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
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.AbstractDQLPlanPhase;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class NestedLoopPhase extends AbstractDQLPlanPhase implements UpstreamPhase {

    public static final ExecutionPhaseFactory<NestedLoopPhase> FACTORY = new ExecutionPhaseFactory<NestedLoopPhase>() {
        @Override
        public NestedLoopPhase create() {
            return new NestedLoopPhase();
        }
    };

    private Set<String> executionNodes;
    private MergePhase leftMergePhase;
    private MergePhase rightMergePhase;
    private DistributionType distributionType = DistributionType.SAME_NODE;

    public NestedLoopPhase() {}

    public NestedLoopPhase(UUID jobId,
                           int executionNodeId,
                           String name,
                           List<Projection> projections,
                           MergePhase leftMergePhase,
                           Collection<? extends DataType> leftOutputTypes,
                           MergePhase rightMergePhase,
                           Collection<? extends DataType> rightOutputTypes,
                           Set<String> executionNodes) {
        super(jobId, executionNodeId, name, projections);
        this.leftMergePhase = leftMergePhase;
        this.rightMergePhase = rightMergePhase;
        this.executionNodes = executionNodes;
        outputTypes = new ArrayList<>(leftOutputTypes.size() + rightOutputTypes.size());
        outputTypes.addAll(leftOutputTypes);
        outputTypes.addAll(rightOutputTypes);
    }

    @Override
    public Type type() {
        return Type.NESTED_LOOP;
    }

    @Override
    public Set<String> executionNodes() {
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

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopPhase(this, context);
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopPhase(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        distributionType = DistributionType.values()[in.readVInt()];

        int numExecutionNodes = in.readVInt();
        if (numExecutionNodes > 0) {
            executionNodes = new HashSet<>(numExecutionNodes);
            for (int i = 0; i < numExecutionNodes; i++) {
                executionNodes.add(in.readString());
            }
        }
        if (in.readBoolean()) {
            leftMergePhase = MergePhase.FACTORY.create();
            leftMergePhase.readFrom(in);
        }
        if (in.readBoolean()) {
            rightMergePhase = MergePhase.FACTORY.create();
            rightMergePhase.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(distributionType.ordinal());

        if (executionNodes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(executionNodes.size());
            for (String node : executionNodes) {
                out.writeString(node);
            }
        }

        if (leftMergePhase == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            leftMergePhase.writeTo(out);
        }
        if (rightMergePhase == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            rightMergePhase.writeTo(out);
        }
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
                .add("executionPhaseId", executionPhaseId())
                .add("name", name())
                .add("outputTypes", outputTypes)
                .add("jobId", jobId())
                .add("executionNodes", executionNodes);
        return helper.toString();
    }

    @Override
    public DistributionType distributionType() {
        return distributionType;
    }

    @Override
    public void distributionType(DistributionType distributionType) {
        this.distributionType = distributionType;
    }
}

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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Iterables;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.sql.tree.JoinType;

public abstract class JoinPhase extends AbstractProjectionsPhase implements UpstreamPhase {

    private final Collection<String> executionNodes;
    private final int numLeftOutputs;
    private final int numRightOutputs;
    private final MergePhase leftMergePhase;
    private final MergePhase rightMergePhase;
    protected final JoinType joinType;

    @Nullable
    private final Symbol joinCondition;

    private DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

    JoinPhase(UUID jobId,
              int executionNodeId,
              String name,
              List<Projection> projections,
              @Nullable MergePhase leftMergePhase,
              @Nullable MergePhase rightMergePhase,
              int numLeftOutputs,
              int numRightOutputs,
              Collection<String> executionNodes,
              JoinType joinType,
              @Nullable Symbol joinCondition) {
        super(jobId, executionNodeId, name, projections);
        Projection lastProjection = Iterables.getLast(projections, null);
        assert lastProjection != null : "lastProjection must not be null";
        assert joinCondition == null || !joinCondition.any(Symbol.IS_COLUMN)
            : "joinCondition must not contain columns: " + joinCondition;
        outputTypes = Symbols.typeView(lastProjection.outputs());
        this.leftMergePhase = leftMergePhase;
        this.rightMergePhase = rightMergePhase;
        this.numLeftOutputs = numLeftOutputs;
        this.numRightOutputs = numRightOutputs;
        this.executionNodes = executionNodes;
        this.joinType = joinType;
        this.joinCondition = joinCondition;
    }

    @Override
    public abstract Type type();

    @Override
    public Collection<String> nodeIds() {
        return Objects.requireNonNullElseGet(executionNodes, Set::of);
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

    JoinPhase(StreamInput in) throws IOException {
        super(in);
        distributionInfo = new DistributionInfo(in);

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
        numLeftOutputs = in.readVInt();
        numRightOutputs = in.readVInt();

        joinCondition = Symbol.nullableFromStream(in);
        joinType = JoinType.values()[in.readVInt()];
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
        out.writeVInt(numLeftOutputs);
        out.writeVInt(numRightOutputs);

        Symbol.nullableToStream(joinCondition, out);

        out.writeVInt(joinType.ordinal());
    }

    @Override
    public String toString() {
        return "JoinPhase{" +
               "executionNodes=" + executionNodes +
               ", executionPhaseId=" + phaseId() +
               ", name=" + name() +
               ", joinType=" + joinType +
               ", joinCondition=" + joinCondition +
               ", outputTypes=" + outputTypes +
               ", jobId=" + jobId() +
               ", executionNodes=" + executionNodes +
               '}';
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        JoinPhase joinPhase = (JoinPhase) o;
        return numLeftOutputs == joinPhase.numLeftOutputs &&
               numRightOutputs == joinPhase.numRightOutputs &&
               Objects.equals(executionNodes, joinPhase.executionNodes) &&
               Objects.equals(leftMergePhase, joinPhase.leftMergePhase) &&
               Objects.equals(rightMergePhase, joinPhase.rightMergePhase) &&
               joinType == joinPhase.joinType &&
               Objects.equals(joinCondition, joinPhase.joinCondition) &&
               Objects.equals(distributionInfo, joinPhase.distributionInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), executionNodes, numLeftOutputs, numRightOutputs,
            leftMergePhase, rightMergePhase, joinType, joinCondition, distributionInfo);
    }
}

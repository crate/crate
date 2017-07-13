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

package io.crate.planner.node.dql;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.analyze.symbol.Symbols;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * A plan node which merges results from upstreams
 */
public class MergePhase extends AbstractProjectionsPhase implements UpstreamPhase {

    private final Collection<? extends DataType> inputTypes;
    private final int numUpstreams;
    private final Collection<String> executionNodes;

    private DistributionInfo distributionInfo;

    /**
     * expects sorted input and produces sorted output
     */
    @Nullable private PositionalOrderBy positionalOrderBy;

    public MergePhase(UUID jobId,
                      int executionNodeId,
                      String name,
                      int numUpstreams,
                      Collection<String> executionNodes,
                      Collection<? extends DataType> inputTypes,
                      List<Projection> projections,
                      DistributionInfo distributionInfo,
                      @Nullable PositionalOrderBy positionalOrderBy) {
        super(jobId, executionNodeId, name, projections);
        this.inputTypes = inputTypes;
        this.numUpstreams = numUpstreams;
        this.distributionInfo = distributionInfo;
        if (projections.isEmpty()) {
            outputTypes = Lists.newArrayList(inputTypes);
        } else {
            outputTypes = Symbols.typeView(Iterables.getLast(projections).outputs());
        }
        this.positionalOrderBy = positionalOrderBy;
        this.executionNodes = executionNodes;
    }

    @Override
    public Type type() {
        return Type.MERGE;
    }

    @Override
    public Collection<String> nodeIds() {
        return executionNodes;
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    public int numUpstreams() {
        return numUpstreams;
    }

    public Collection<? extends DataType> inputTypes() {
        return inputTypes;
    }

    @Nullable
    public PositionalOrderBy orderByPositions() {
        return positionalOrderBy;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitMergePhase(this, context);
    }

    public MergePhase(StreamInput in) throws IOException {
        super(in);
        distributionInfo = DistributionInfo.fromStream(in);
        numUpstreams = in.readVInt();

        int numCols = in.readVInt();
        if (numCols > 0) {
            List<DataType> inputTypes = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                inputTypes.add(DataTypes.fromStream(in));
            }
            this.inputTypes = inputTypes;
        } else {
            inputTypes = Collections.emptyList();
        }
        int numExecutionNodes = in.readVInt();
        if (numExecutionNodes > 0) {
            executionNodes = new HashSet<>(numExecutionNodes);
            for (int i = 0; i < numExecutionNodes; i++) {
                executionNodes.add(in.readString());
            }
        } else {
            executionNodes = Collections.emptySet();
        }

        positionalOrderBy = PositionalOrderBy.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        distributionInfo.writeTo(out);
        out.writeVInt(numUpstreams);

        int numCols = inputTypes.size();
        out.writeVInt(numCols);
        for (DataType inputType : inputTypes) {
            DataTypes.toStream(inputType, out);
        }

        out.writeVInt(executionNodes.size());
        for (String node : executionNodes) {
            out.writeString(node);
        }

        PositionalOrderBy.toStream(positionalOrderBy, out);
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
            .add("executionPhaseId", phaseId())
            .add("name", name())
            .add("projections", projections)
            .add("outputTypes", outputTypes)
            .add("jobId", jobId())
            .add("numUpstreams", numUpstreams)
            .add("nodeOperations", executionNodes)
            .add("inputTypes", inputTypes)
            .add("orderBy", positionalOrderBy);
        return helper.toString();
    }
}

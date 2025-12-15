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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jspecify.annotations.Nullable;

import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * A plan node which merges results from upstreams
 */
public class MergePhase extends AbstractProjectionsPhase implements UpstreamPhase {

    private final List<? extends DataType<?>> inputTypes;

    private final int numUpstreams;

    /** The number of different inputs, e.g. Union has inputs from two Collect phases */
    private final int numInputs;
    private final Collection<String> executionNodes;
    private final Collection<String> upstreamNodes;

    private DistributionInfo distributionInfo;

    /**
     * expects sorted input and produces sorted output
     */
    @Nullable private PositionalOrderBy positionalOrderBy;

    /**
     * Creates a MergePhase to combine the results from multiple node operations.
     * @param jobId The JobID of the entire execution.
     * @param executionNodeId A unique execution id for this phase.
     * @param name The name of the MergePhase.
     * @param numUpstreams The number of upstreams to expect data from. Typically the number of upstream nodes.
     *                     But can be more (e.g. in case of union)
     * @param numInputs The number of different inputs to read data from which is equal to
     *                  the number of upstream phases.
     * @param executionNodes The nodes where this MergePhase executes.
     * @param inputTypes The types of the input rows.
     * @param projections The projections to apply when merging.
     * @param upstreamNodes the nodeIds of the nodes expected to provide results to this mergePhase.
     *                      Can be fewer than {@code numUpstreams} if one node is running multiple operations.
     * @param distributionInfo The default strategy to use when distributing the results of the MergePhase.
     * @param positionalOrderBy The order by positions on which the input is pre-sorted on; setting this
     *                          will result in a sorted merge.
     */
    public MergePhase(UUID jobId,
                      int executionNodeId,
                      String name,
                      int numUpstreams,
                      int numInputs,
                      Collection<String> executionNodes,
                      List<? extends DataType<?>> inputTypes,
                      List<Projection> projections,
                      Collection<String> upstreamNodes,
                      DistributionInfo distributionInfo,
                      @Nullable PositionalOrderBy positionalOrderBy) {
        super(jobId, executionNodeId, name, projections);
        this.numInputs = numInputs;
        this.inputTypes = inputTypes;
        // numUpstreams can be different than upstreamNodes.size() - e.g. in union cases
        this.numUpstreams = numUpstreams;
        this.upstreamNodes = upstreamNodes;
        this.distributionInfo = distributionInfo;
        if (projections.isEmpty()) {
            outputTypes = List.copyOf(inputTypes);
        } else {
            outputTypes = Symbols.typeView(projections.get(projections.size() - 1).outputs());
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

    /**
     * The number of inputs of the MergePhase. A typical MergePhase has only
     * one input. {@link io.crate.planner.operators.Union} has two.
     */
    public int numInputs() {
        return numInputs;
    }

    public List<? extends DataType<?>> inputTypes() {
        return inputTypes;
    }

    @Nullable
    public PositionalOrderBy orderByPositions() {
        return positionalOrderBy;
    }

    public Collection<String> upstreamNodes() {
        return upstreamNodes;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitMergePhase(this, context);
    }

    public MergePhase(StreamInput in) throws IOException {
        super(in);
        distributionInfo = new DistributionInfo(in);
        numUpstreams = in.readVInt();
        numInputs = in.readVInt();

        int numCols = in.readVInt();
        if (numCols > 0) {
            List<DataType<?>> inputTypes = new ArrayList<>(numCols);
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
        if (in.getVersion().onOrAfter(Version.V_5_10_5)) {
            upstreamNodes = in.readStringList();
        } else {
            upstreamNodes = List.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        distributionInfo.writeTo(out);
        out.writeVInt(numUpstreams);
        out.writeVInt(numInputs);

        int numCols = inputTypes.size();
        out.writeVInt(numCols);
        for (DataType<?> inputType : inputTypes) {
            DataTypes.toStream(inputType, out);
        }

        out.writeVInt(executionNodes.size());
        for (String node : executionNodes) {
            out.writeString(node);
        }

        PositionalOrderBy.toStream(positionalOrderBy, out);
        if (out.getVersion().onOrAfter(Version.V_5_10_5)) {
            out.writeStringCollection(upstreamNodes);
        }
    }

    @Override
    public String toString() {
        return "MergePhase{" +
               "executionPhaseId=" + phaseId() +
               ", name=" + name() +
               ", projections=" + projections +
               ", outputTypes=" + outputTypes +
               ", jobId=" + jobId() +
               ", numUpstreams=" + numUpstreams +
               ", numInputs=" + numInputs +
               ", nodeOperations=" + executionNodes +
               ", inputTypes=" + inputTypes +
               ", orderBy=" + positionalOrderBy +
               '}';
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
        MergePhase that = (MergePhase) o;
        return numUpstreams == that.numUpstreams &&
               numInputs == that.numInputs &&
               Objects.equals(inputTypes, that.inputTypes) &&
               Objects.equals(executionNodes, that.executionNodes) &&
               Objects.equals(distributionInfo, that.distributionInfo) &&
               Objects.equals(positionalOrderBy, that.positionalOrderBy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inputTypes, numUpstreams, numInputs, executionNodes,
            distributionInfo, positionalOrderBy);
    }
}

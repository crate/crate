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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.planner.node.ExecutionPhaseVisitor;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbols;
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
public class MergePhase extends AbstractDQLPlanPhase {

    public static final ExecutionPhaseFactory<MergePhase> FACTORY = new ExecutionPhaseFactory<MergePhase>() {
        @Override
        public MergePhase create() {
            return new MergePhase();
        }
    };

    private Collection<? extends DataType> inputTypes;
    private int numUpstreams;
    private Set<String> executionNodes;

    /**
     * expects sorted input and produces sorted output
     */
    private boolean sortedInputOutput = false;
    private int[] orderByIndices;
    private boolean[] reverseFlags;
    private Boolean[] nullsFirst;

    public MergePhase() {
        numUpstreams = 0;
    }

    public MergePhase(UUID jobId,
                      int executionNodeId,
                      String name,
                      int numUpstreams,
                      Collection<? extends DataType> inputTypes,
                      List<Projection> projections) {
        super(jobId, executionNodeId, name, projections);
        this.inputTypes = inputTypes;
        this.numUpstreams = numUpstreams;
        if (projections.isEmpty()) {
            outputTypes = Lists.newArrayList(inputTypes);
        } else {
            outputTypes = Symbols.extractTypes(Iterables.getLast(projections).outputs());
        }
    }

    public static MergePhase sortedMergeNode(UUID jobId,
                                            Collection<? extends DataType> inputTypes,
                                            List<Projection> projections,
                                            int executionNodeId,
                                            String name,
                                            int numUpstreams,
                                            int[] orderByIndices,
                                            boolean[] reverseFlags,
                                            Boolean[] nullsFirst) {
        Preconditions.checkArgument(
                orderByIndices.length == reverseFlags.length && reverseFlags.length == nullsFirst.length,
                "ordering parameters must be of the same length");
        MergePhase mergeNode = new MergePhase(jobId, executionNodeId, name, numUpstreams, inputTypes, projections);
        mergeNode.sortedInputOutput = true;
        mergeNode.orderByIndices = orderByIndices;
        mergeNode.reverseFlags = reverseFlags;
        mergeNode.nullsFirst = nullsFirst;
        return mergeNode;
    }

    @Override
    public Type type() {
        return Type.MERGE;
    }

    @Override
    public Set<String> executionNodes() {
        if (executionNodes == null) {
            return ImmutableSet.of();
        } else {
            return executionNodes;
        }
    }

    public void executionNodes(Set<String> executionNodes) {
        this.executionNodes = executionNodes;
    }

    public int numUpstreams() {
        return numUpstreams;
    }

    public Collection<? extends DataType> inputTypes() {
        return inputTypes;
    }

    public boolean sortedInputOutput() {
        return sortedInputOutput;
    }

    @Nullable
    public int[] orderByIndices() {
        return orderByIndices;
    }

    @Nullable
    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    @Nullable
    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitMergeNode(this, context);
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitMergeNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        numUpstreams = in.readVInt();

        int numCols = in.readVInt();
        if (numCols > 0) {
            List<DataType> inputTypes = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                inputTypes.add(DataTypes.fromStream(in));
            }
            this.inputTypes = inputTypes;
        }
        int numExecutionNodes = in.readVInt();

        if (numExecutionNodes > 0) {
            executionNodes = new HashSet<>(numExecutionNodes);
            for (int i = 0; i < numExecutionNodes; i++) {
                executionNodes.add(in.readString());
            }
        }

        sortedInputOutput = in.readBoolean();
        if (sortedInputOutput) {
            int orderByIndicesLength = in.readVInt();
            orderByIndices = new int[orderByIndicesLength];
            reverseFlags = new boolean[orderByIndicesLength];
            nullsFirst = new Boolean[orderByIndicesLength];
            for (int i = 0; i < orderByIndicesLength; i++) {
                orderByIndices[i] = in.readVInt();
                reverseFlags[i] = in.readBoolean();
                nullsFirst[i] = in.readOptionalBoolean();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numUpstreams);

        int numCols = inputTypes.size();
        out.writeVInt(numCols);
        for (DataType inputType : inputTypes) {
            DataTypes.toStream(inputType, out);
        }

        if (executionNodes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(executionNodes.size());
            for (String node : executionNodes) {
                out.writeString(node);
            }
        }

        out.writeBoolean(sortedInputOutput);
        if (sortedInputOutput) {
            out.writeVInt(orderByIndices.length);
            for (int i = 0; i < orderByIndices.length; i++) {
                out.writeVInt(orderByIndices[i]);
                out.writeBoolean(reverseFlags[i]);
                out.writeOptionalBoolean(nullsFirst[i]);
            }
        }
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
                .add("executionPhaseId", executionPhaseId())
                .add("name", name())
                .add("projections", projections)
                .add("outputTypes", outputTypes)
                .add("jobId", jobId())
                .add("numUpstreams", numUpstreams)
                .add("nodeOperations", executionNodes)
                .add("inputTypes", inputTypes)
                .add("sortedInputOutput", sortedInputOutput);
        if (sortedInputOutput) {
            helper.add("orderByIndices", Arrays.toString(orderByIndices))
                  .add("reverseFlags", Arrays.toString(reverseFlags))
                  .add("nullsFirst", Arrays.toString(nullsFirst));
        }
        return helper.toString();
    }
}

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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.planner.Planner;
import io.crate.planner.ResultDescription;
import io.crate.planner.consumer.OrderByPositionVisitor;
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
public class MergePhase extends AbstractProjectionsPhase implements UpstreamPhase, ResultDescription {

    public static final ExecutionPhaseFactory<MergePhase> FACTORY = new ExecutionPhaseFactory<MergePhase>() {
        @Override
        public MergePhase create() {
            return new MergePhase();
        }
    };

    private Collection<? extends DataType> inputTypes;
    private int numUpstreams;
    private DistributionInfo distributionInfo;
    private Collection<String> executionNodes;

    /**
     * expects sorted input and produces sorted output
     */
    private boolean sortedInputOutput = false;
    private int[] orderByIndices;
    private boolean[] reverseFlags;
    private Boolean[] nullsFirst;

    private MergePhase() {
    }

    public MergePhase(UUID jobId,
                      int executionNodeId,
                      String name,
                      int numUpstreams,
                      Collection<? extends DataType> inputTypes,
                      List<Projection> projections,
                      DistributionInfo distributionInfo
    ) {
        super(jobId, executionNodeId, name, projections);
        this.inputTypes = inputTypes;
        this.numUpstreams = numUpstreams;
        this.distributionInfo = distributionInfo;
        if (projections.isEmpty()) {
            outputTypes = Lists.newArrayList(inputTypes);
        } else {
            outputTypes = Symbols.extractTypes(Iterables.getLast(projections).outputs());
        }
    }

    public static MergePhase localMerge(UUID jobId,
                                        int executionPhaseId,
                                        List<Projection> projections,
                                        int numUpstreams,
                                        Collection<? extends DataType> inputTypes) {
        return new MergePhase(
            jobId,
            executionPhaseId,
            "localMerge",
            numUpstreams,
            inputTypes,
            projections,
            DistributionInfo.DEFAULT_SAME_NODE
        );
    }

    public static MergePhase sortedMerge(UUID jobId,
                                         int executionPhaseId,
                                         OrderBy orderBy,
                                         List<? extends Symbol> sourceSymbols,
                                         @Nullable List<? extends Symbol> orderBySymbolOverwrite,
                                         List<Projection> projections,
                                         int numUpstreams,
                                         Collection<? extends DataType> inputTypes) {

        int[] orderByIndices = OrderByPositionVisitor.orderByPositions(
            MoreObjects.firstNonNull(orderBySymbolOverwrite, orderBy.orderBySymbols()),
            sourceSymbols
        );
        assert orderBy.reverseFlags().length == orderByIndices.length :
            "length of reverseFlags and orderByIndices must match";

        MergePhase mergeNode = new MergePhase(
            jobId,
            executionPhaseId,
            "sortedLocalMerge",
            numUpstreams,
            inputTypes,
            projections,
            DistributionInfo.DEFAULT_SAME_NODE
        );
        mergeNode.sortedInputOutput = true;
        mergeNode.orderByIndices = orderByIndices;
        mergeNode.reverseFlags = orderBy.reverseFlags();
        mergeNode.nullsFirst = orderBy.nullsFirst();
        return mergeNode;
    }

    /**
     * @param orderBySymbols Can be used to override orderBySymbols of {@param orderBy}
     * @param inputTypes Can be used if available to avoid extracting them again from {@param inputs}
     */
    @Nullable
    public static MergePhase mergePhase(Planner.Context plannerContext,
                                        Collection<String> executionNodes,
                                        int upstreamPhaseExecutionNodesSize,
                                        @Nullable OrderBy orderBy,
                                        @Nullable List<? extends Symbol> orderBySymbols,
                                        List<Projection> projections,
                                        List<Symbol> inputs,
                                        @Nullable List<DataType> inputTypes) {
        MergePhase mergePhase;
        if (orderBy != null) {
            mergePhase = MergePhase.sortedMerge(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                orderBy,
                inputs,
                orderBySymbols != null ? orderBySymbols : orderBy.orderBySymbols(),
                projections,
                upstreamPhaseExecutionNodesSize,
                inputTypes != null ? inputTypes : Symbols.extractTypes(inputs)
            );
        } else {
            // no sorting needed
            mergePhase = MergePhase.localMerge(
                plannerContext.jobId(),
                plannerContext.nextExecutionPhaseId(),
                projections,
                upstreamPhaseExecutionNodesSize,
                inputTypes != null ? inputTypes : Symbols.extractTypes(inputs)
            );
        }
        mergePhase.executionNodes(executionNodes);
        return mergePhase;
    }

    @Override
    public Type type() {
        return Type.MERGE;
    }

    @Override
    public Collection<String> executionNodes() {
        if (executionNodes == null) {
            return ImmutableSet.of();
        } else {
            return executionNodes;
        }
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    public void executionNodes(Collection<String> executionNodes) {
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
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitMergePhase(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        distributionInfo = DistributionInfo.fromStream(in);
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
        distributionInfo.writeTo(out);
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

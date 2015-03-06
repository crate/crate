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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

/**
 * A plan node which merges results from upstreams
 */
public class MergeNode extends AbstractDQLPlanNode {

    private List<DataType> inputTypes;
    private int numUpstreams;
    private Set<String> executionNodes;
    private UUID jobId;
    private IntObjectOpenHashMap<String> jobSearchContextIdToNode;

    public MergeNode() {
        numUpstreams = 0;
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

    public MergeNode(String id, int numUpstreams) {
        super(id);
        this.numUpstreams = numUpstreams;
    }

    public int numUpstreams() {
        return numUpstreams;
    }

    public UUID jobId() {
        return jobId;
    }

    public void jobId(UUID jobId) {
        this.jobId = jobId;
    }

    public List<DataType> inputTypes() {
        return inputTypes;
    }

    public void inputTypes(List<DataType> inputTypes) {
        this.inputTypes = inputTypes;
    }

    public void jobSearchContextIdToNode(IntObjectOpenHashMap<String> jobSearchContextIdToNode) {
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
    }

    public IntObjectOpenHashMap<String> jobSearchContextIdToNode() {
        return jobSearchContextIdToNode;
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitMergeNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        numUpstreams = in.readVInt();
        jobId = new UUID(in.readLong(), in.readLong());

        int numCols = in.readVInt();
        if (numCols > 0) {
            inputTypes = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                inputTypes.add(DataTypes.fromStream(in));
            }
        }
        int numExecutionNodes = in.readVInt();

        if (numExecutionNodes > 0) {
            executionNodes = new HashSet<>(numExecutionNodes);
            for (int i = 0; i < numExecutionNodes; i++) {
                executionNodes.add(in.readString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(numUpstreams);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());

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
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id())
                .add("projections", projections)
                .add("outputTypes", outputTypes)
                .add("jobId", jobId)
                .add("numUpstreams", numUpstreams)
                .add("executionNodes", executionNodes)
                .add("inputTypes", inputTypes)
                .toString();
    }
}

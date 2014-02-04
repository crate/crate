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

package io.crate.planner.node;

import com.google.common.collect.ImmutableSet;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A plan node which merges results from upstreams
 */
public class MergeNode extends PlanNode {

    private List<DataType> inputTypes;
    private int numUpstreams;
    private Set<String> executionNodes;

    public MergeNode() {
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

    public void numUpstreams(int numUpstreams) {
        this.numUpstreams = numUpstreams;
    }

    public List<DataType> inputTypes() {
        return inputTypes;
    }

    public void inputTypes(List<DataType> inputTypes) {
        this.inputTypes = inputTypes;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitMergeNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        int numCols = in.readVInt();
        if (numCols > 0) {
            inputTypes = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                inputTypes.add(DataType.values()[in.readVInt()]);
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

        int numCols = inputTypes.size();
        out.writeVInt(numCols);
        for (int i = 0; i < numCols; i++) {
            out.writeVInt(inputTypes.get(i).ordinal());
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

}

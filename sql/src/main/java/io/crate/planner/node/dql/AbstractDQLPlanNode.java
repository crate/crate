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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.crate.planner.node.ExecutionNode;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class AbstractDQLPlanNode implements DQLPlanNode, Streamable, ExecutionNode {

    private UUID jobId;
    private int executionNodeId;
    private String name;
    protected List<Projection> projections = ImmutableList.of();
    protected List<DataType> outputTypes = ImmutableList.of();
    private List<DataType> inputTypes;

    public AbstractDQLPlanNode() {

    }

    protected AbstractDQLPlanNode(int executionNodeId, String name) {
        this.executionNodeId = executionNodeId;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public UUID jobId() {
        return jobId;
    }

    @Override
    public void jobId(UUID jobId) {
        this.jobId = jobId;
    }

    @Override
    public int executionNodeId() {
        return executionNodeId;
    }

    @Override
    public boolean hasProjections() {
        return projections != null && projections.size() > 0;
    }

    @Override
    public List<Projection> projections() {
        return projections;
    }

    public void projections(List<Projection> projections) {
        this.projections = projections;
    }

    @Override
    public void addProjection(Projection projection) {
        List<Projection> projections = new ArrayList<>(this.projections);
        projections.add(projection);
        this.projections = ImmutableList.copyOf(projections);
    }

    public Optional<Projection> finalProjection() {
        if (projections.size() == 0) {
            return Optional.absent();
        } else {
            return Optional.of(projections.get(projections.size()-1));
        }
    }


    public void outputTypes(List<DataType> outputTypes) {
        this.outputTypes = outputTypes;
    }

    public List<DataType> outputTypes() {
        return outputTypes;
    }

    @Override
    public void inputTypes(List<DataType> dataTypes) {
        this.inputTypes = dataTypes;
    }

    @Override
    public List<DataType> inputTypes() {
        return inputTypes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        jobId = new UUID(in.readLong(), in.readLong());
        executionNodeId = in.readVInt();

        int numCols = in.readVInt();
        if (numCols > 0) {
            outputTypes = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                outputTypes.add(DataTypes.fromStream(in));
            }
        }

        int numProjections = in.readVInt();
        if (numProjections > 0) {
            projections = new ArrayList<>(numProjections);
            for (int i = 0; i < numProjections; i++) {
                projections.add(Projection.fromStream(in));
            }
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        assert jobId != null : "jobId must not be null";
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(executionNodeId);

        int numCols = outputTypes.size();
        out.writeVInt(numCols);
        for (int i = 0; i < numCols; i++) {
            DataTypes.toStream(outputTypes.get(i), out);
        }

        if (hasProjections()) {
            out.writeVInt(projections.size());
            for (Projection p : projections) {
                Projection.toStream(p, out);
            }
        } else {
            out.writeVInt(0);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractDQLPlanNode node = (AbstractDQLPlanNode) o;

        return !(name != null ? !name.equals(node.name) : node.name != null);

    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("projections", projections)
                .add("outputTypes", outputTypes)
                .toString();
    }
}

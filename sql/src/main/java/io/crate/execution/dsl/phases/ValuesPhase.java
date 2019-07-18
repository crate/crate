/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dsl.phases;

import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.engine.pipeline.TopN;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.ExecutionPlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public final class ValuesPhase implements UpstreamPhase, ExecutionPlan, ResultDescription {

    private final UUID jobId;
    private final int phaseId;
    private final List<Symbol> outputs;
    private final List<List<Symbol>> boundRows;
    private final Symbol filter;
    private final String coordinatorNodeId;
    private final List<Projection> projections;

    private DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

    public ValuesPhase(UUID jobId,
                       int phaseId,
                       String coordinatorNodeId,
                       List<Symbol> outputs,
                       List<List<Symbol>> boundRows,
                       Symbol filter) {
        this.jobId = jobId;
        this.phaseId = phaseId;
        this.coordinatorNodeId = coordinatorNodeId;
        this.outputs = outputs;
        this.boundRows = boundRows;
        this.filter = filter;
        this.projections = new ArrayList<>();
    }

    public ValuesPhase(StreamInput in) throws IOException {
        this.jobId = new UUID(in.readLong(), in.readLong());
        this.phaseId = in.readVInt();
        this.coordinatorNodeId = in.readString();
        this.outputs = Symbols.listFromStream(in);
        this.filter = Symbols.fromStream(in);

        int numRows = in.readVInt();
        boundRows = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; i++) {
            boundRows.add(Symbols.listFromStream(in));
        }
        this.projections = List.of();
    }

    public List<List<Symbol>> rows() {
        return boundRows;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(phaseId);
        out.writeString(coordinatorNodeId);
        Symbols.toStream(outputs, out);
        Symbols.toStream(filter, out);

        out.writeVInt(boundRows.size());
        for (List<Symbol> row : boundRows) {
            Symbols.toStream(row, out);
        }
    }

    @Override
    public Type type() {
        return Type.VALUES_PHASE;
    }

    @Override
    public String name() {
        return "VALUES";
    }

    @Override
    public int phaseId() {
        return phaseId;
    }

    @Override
    public Collection<String> nodeIds() {
        return List.of(coordinatorNodeId);
    }

    @Nullable
    @Override
    public PositionalOrderBy orderBy() {
        return null;
    }

    @Override
    public int limit() {
        return TopN.NO_LIMIT;
    }

    @Override
    public int maxRowsPerNode() {
        return boundRows.size();
    }

    @Override
    public int offset() {
        return 0;
    }

    @Override
    public int numOutputs() {
        return outputs.size();
    }

    @Override
    public List<DataType> streamOutputs() {
        return Symbols.typeView(outputs);
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitValues(this, context);
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
    public <C, R> R accept(ExecutionPlanVisitor<C, R> visitor, C context) {
        return visitor.visitValues(this, context);
    }

    @Override
    public void addProjection(Projection projection) {
        projections.add(projection);
    }

    @Override
    public void addProjection(Projection projection,
                              int unfinishedLimit,
                              int unfinishedOffset,
                              @Nullable PositionalOrderBy unfinishedOrderBy) {
        projections.add(projection);
    }

    @Override
    public ResultDescription resultDescription() {
        return this;
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }
}

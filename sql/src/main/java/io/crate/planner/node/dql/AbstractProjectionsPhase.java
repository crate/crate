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
import com.google.common.collect.Iterables;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.planner.node.ExecutionPhase;
import io.crate.planner.projection.Projection;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public abstract class AbstractProjectionsPhase implements ExecutionPhase {

    private UUID jobId;
    private int executionPhaseId;
    private String name;
    protected List<Projection> projections = ImmutableList.of();
    protected List<DataType> outputTypes = ImmutableList.of();

    protected AbstractProjectionsPhase(UUID jobId, int executionPhaseId, String name, List<Projection> projections) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.name = name;
        this.projections = projections;
    }

    protected static List<DataType> extractOutputTypes(List<Symbol> outputs, List<Projection> projections) {
        Projection lastProjection = Iterables.getLast(projections, null);
        if (lastProjection == null) {
            return Symbols.extractTypes(outputs);
        } else {
            return Symbols.extractTypes(lastProjection.outputs());
        }
    }

    public String name() {
        return name;
    }

    public UUID jobId() {
        return jobId;
    }

    @Override
    public int phaseId() {
        return executionPhaseId;
    }

    public boolean hasProjections() {
        return projections != null && projections.size() > 0;
    }

    public List<Projection> projections() {
        return projections;
    }

    public void addProjection(Projection projection) {
        List<Projection> projections = new ArrayList<>(this.projections);
        projections.add(projection);
        this.projections = ImmutableList.copyOf(projections);
        outputTypes = Symbols.extractTypes(projection.outputs());
    }

    public Optional<Projection> finalProjection() {
        if (projections.size() == 0) {
            return Optional.absent();
        } else {
            return Optional.of(projections.get(projections.size() - 1));
        }
    }

    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        for (Projection projection : projections) {
            projection.replaceSymbols(replaceFunction);
        }
    }

    public List<DataType> outputTypes() {
        return outputTypes;
    }

    protected AbstractProjectionsPhase(StreamInput in) throws IOException {
        name = in.readString();
        jobId = new UUID(in.readLong(), in.readLong());
        executionPhaseId = in.readVInt();

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
        out.writeVInt(executionPhaseId);

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

        AbstractProjectionsPhase node = (AbstractProjectionsPhase) o;

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

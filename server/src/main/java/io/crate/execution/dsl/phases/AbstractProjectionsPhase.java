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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.execution.dsl.projection.Projection;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public abstract class AbstractProjectionsPhase implements ExecutionPhase {


    private final UUID jobId;
    private final int executionPhaseId;
    private final String name;
    protected final List<Projection> projections;
    protected List<DataType<?>> outputTypes = List.of();

    protected AbstractProjectionsPhase(UUID jobId,
                                       int executionPhaseId,
                                       String name,
                                       @Nullable Collection<? extends Projection> projections) {
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
        this.name = name;
        this.projections = projections == null ? new ArrayList<>() : new ArrayList<>(projections);
    }

    protected static List<DataType<?>> extractOutputTypes(List<Symbol> outputs, List<Projection> projections) {
        if (projections.isEmpty()) {
            return Symbols.typeView(outputs);
        } else {
            var lastProjection = projections.get(projections.size() - 1);
            return Symbols.typeView(lastProjection.outputs());
        }
    }

    @Override
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

    @Override
    public Streamer<?>[] getStreamers() {
        return DataTypes.getStreamers(outputTypes());
    }

    public boolean hasProjections() {
        return projections.size() > 0;
    }

    public List<Projection> projections() {
        return projections;
    }

    public void addProjection(Projection projection) {
        this.projections.add(projection);
        outputTypes = Symbols.typeView(projection.outputs());
    }

    public Optional<Projection> finalProjection() {
        if (projections.size() == 0) {
            return Optional.empty();
        } else {
            return Optional.of(projections.get(projections.size() - 1));
        }
    }

    public List<DataType<?>> outputTypes() {
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
        } else {
            projections = List.of();
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
        return "AbstractProjectionsPhase{" +
               "name=" + name +
               ", projections=" + projections +
               ", outputTypes=" + outputTypes +
               '}';
    }
}

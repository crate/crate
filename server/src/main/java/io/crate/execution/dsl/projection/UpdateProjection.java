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

package io.crate.execution.dsl.projection;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import io.crate.types.DataTypes;

public class UpdateProjection extends Projection {

    final Symbol uidSymbol;

    private Symbol[] assignments;
    // All values of this list are expected to be a FQN columnIdent.
    private String[] assignmentsColumns;

    private Symbol[] outputs;

    @Nullable
    private Symbol[] returnValues;

    @Nullable
    private Long requiredVersion;

    public UpdateProjection(Symbol uidSymbol,
                            String[] assignmentsColumns,
                            Symbol[] assignments,
                            Symbol[] outputs,
                            @Nullable Symbol[] returnValues,
                            @Nullable Long requiredVersion) {
        this.uidSymbol = uidSymbol;
        this.assignmentsColumns = assignmentsColumns;
        this.assignments = assignments;
        this.returnValues = returnValues;
        assert Arrays.stream(outputs).noneMatch(s -> s.any(Symbol.IS_COLUMN.or(x -> x instanceof SelectSymbol)))
            : "Cannot operate on Reference, Field or SelectSymbol symbols: " + outputs;
        this.outputs = outputs;
        this.requiredVersion = requiredVersion;
    }

    public UpdateProjection(StreamInput in) throws IOException {
        uidSymbol = Symbol.fromStream(in);
        int assignmentColumnsSize = in.readVInt();
        assignmentsColumns = new String[assignmentColumnsSize];
        for (int i = 0; i < assignmentColumnsSize; i++) {
            assignmentsColumns[i] = in.readString();
        }
        int assignmentsSize = in.readVInt();
        assignments = new Symbol[assignmentsSize];
        for (int i = 0; i < assignmentsSize; i++) {
            assignments[i] = Symbol.fromStream(in);
        }
        requiredVersion = in.readVLong();
        if (requiredVersion == 0) {
            requiredVersion = null;
        }
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            int outputSize = in.readVInt();
            outputs = new Symbol[outputSize];
            for (int i = 0; i < outputSize; i++) {
                outputs[i] = Symbol.fromStream(in);
            }
            int returnValuesSize = in.readVInt();
            if (returnValuesSize > 0) {
                returnValues = new Symbol[returnValuesSize];
                for (int i = 0; i < returnValuesSize; i++) {
                    returnValues[i] = Symbol.fromStream(in);
                }
            }
        } else {
            //Outputs should never be null and for BwC reasons
            //the default value in pre 4.1 was a long for a count
            outputs = new Symbol[]{new InputColumn(0, DataTypes.LONG)};
        }
    }

    public Symbol uidSymbol() {
        return uidSymbol;
    }

    @Nullable
    public Symbol[] returnValues() {
        return returnValues;
    }

    public String[] assignmentsColumns() {
        return assignmentsColumns;
    }

    public Symbol[] assignments() {
        return assignments;
    }

    @Nullable
    public Long requiredVersion() {
        return requiredVersion;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.UPDATE;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitUpdateProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return List.of(outputs);
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.SHARD;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateProjection that = (UpdateProjection) o;

        if (!Arrays.equals(assignments, that.assignments)) return false;
        if (!Arrays.equals(assignmentsColumns, that.assignmentsColumns)) return false;
        if (requiredVersion != null ? !requiredVersion.equals(that.requiredVersion) : that.requiredVersion != null)
            return false;
        if (!uidSymbol.equals(that.uidSymbol)) return false;
        if (!Arrays.equals(returnValues, that.returnValues)) return false;
        if (!Arrays.equals(outputs, that.outputs)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(assignments);
        result = 31 * result + Arrays.hashCode(assignmentsColumns);
        result = 31 * result + (requiredVersion != null ? requiredVersion.hashCode() : 0);
        result = 31 * result + uidSymbol.hashCode();
        result = 31 * result + Arrays.hashCode(returnValues);
        result = 31 * result + Arrays.hashCode(outputs);

        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(uidSymbol, out);
        out.writeVInt(assignmentsColumns.length);
        for (int i = 0; i < assignmentsColumns.length; i++) {
            out.writeString(assignmentsColumns[i]);
        }
        out.writeVInt(assignments.length);
        for (int i = 0; i < assignments.length; i++) {
            Symbol.toStream(assignments[i], out);
        }
        if (requiredVersion == null) {
            out.writeVLong(0);
        } else {
            out.writeVLong(requiredVersion);
        }
        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            out.writeVInt(outputs.length);
            for (int i = 0; i < outputs.length; i++) {
                Symbol.toStream(outputs[i], out);
            }
            if (returnValues != null) {
                out.writeVInt(returnValues.length);
                for (int i = 0; i < returnValues.length; i++) {
                    Symbol.toStream(returnValues[i], out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }
}

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.projection;

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;

public class UpdateProjection extends DMLProjection {

    private Symbol[] assignments;
    // All values of this list are expected to be a FQN columnIdent.
    private String[] assignmentsColumns;
    @Nullable
    private Long requiredVersion;

    public UpdateProjection(Symbol uidSymbol,
                            String[] assignmentsColumns,
                            Symbol[] assignments,
                            @Nullable Long requiredVersion) {
        super(uidSymbol);
        this.assignmentsColumns = assignmentsColumns;
        this.assignments = assignments;
        this.requiredVersion = requiredVersion;
    }

    public UpdateProjection(StreamInput in) throws IOException {
        super(in);
        int assignmentColumnsSize = in.readVInt();
        assignmentsColumns = new String[assignmentColumnsSize];
        for (int i = 0; i < assignmentColumnsSize; i++) {
            assignmentsColumns[i] = in.readString();
        }
        int assignmentsSize = in.readVInt();
        assignments = new Symbol[assignmentsSize];
        for (int i = 0; i < assignmentsSize; i++) {
            assignments[i] = Symbols.fromStream(in);
        }
        requiredVersion = in.readVLong();
        if (requiredVersion == 0) {
            requiredVersion = null;
        }
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
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        super.replaceSymbols(replaceFunction);
        for (int i = 0; i < assignments.length; i++) {
            assignments[i] = replaceFunction.apply(assignments[i]);
        }
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateProjection that = (UpdateProjection) o;

        if (!Arrays.equals(assignments, that.assignments)) return false;
        if (!Arrays.equals(assignmentsColumns, that.assignmentsColumns)) return false;
        if (requiredVersion != null ? !requiredVersion.equals(that.requiredVersion) : that.requiredVersion != null)
            return false;
        if (!uidSymbol.equals(that.uidSymbol)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Arrays.hashCode(assignments);
        result = 31 * result + Arrays.hashCode(assignmentsColumns);
        result = 31 * result + (requiredVersion != null ? requiredVersion.hashCode() : 0);
        result = 31 * result + uidSymbol.hashCode();
        return result;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(assignmentsColumns.length);
        for (int i = 0; i < assignmentsColumns.length; i++) {
            out.writeString(assignmentsColumns[i]);
        }
        out.writeVInt(assignments.length);
        for (int i = 0; i < assignments.length; i++) {
            Symbols.toStream(assignments[i], out);
        }
        if (requiredVersion == null) {
            out.writeVLong(0);
        } else {
            out.writeVLong(requiredVersion);
        }
    }
}

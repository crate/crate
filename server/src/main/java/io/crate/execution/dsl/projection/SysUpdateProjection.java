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

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.jetbrains.annotations.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SysUpdateProjection extends Projection {

    private final Symbol uidSymbol;

    private Map<Reference, Symbol> assignments;

    private Symbol[] outputs;

    @Nullable
    private Symbol[] returnValues;

    public SysUpdateProjection(Symbol uidSymbol,
                               Map<Reference, Symbol> assignments,
                               Symbol[] outputs,
                               @Nullable Symbol[] returnValues
    ) {
        this.uidSymbol = uidSymbol;
        this.assignments = assignments;
        this.returnValues = returnValues;
        assert Arrays.stream(outputs).noneMatch(s -> SymbolVisitors.any(Symbols.IS_COLUMN.or(x -> x instanceof SelectSymbol), s))
            : "Cannot operate on Reference, Field or SelectSymbol symbols: " + outputs;
        this.outputs = outputs;
    }

    public SysUpdateProjection(StreamInput in) throws IOException {
        uidSymbol = Symbols.fromStream(in);
        int numAssignments = in.readVInt();
        assignments = new HashMap<>(numAssignments, 1.0f);
        for (int i = 0; i < numAssignments; i++) {
            assignments.put(Reference.fromStream(in), Symbols.fromStream(in));
        }
        if (in.getVersion().onOrAfter(Version.V_4_2_0)) {
            int outputSize = in.readVInt();
            outputs = new Symbol[outputSize];
            for (int i = 0; i < outputSize; i++) {
                outputs[i] = Symbols.fromStream(in);
            }
            int returnValuesSize = in.readVInt();
            if (returnValuesSize > 0) {
                returnValues = new Symbol[returnValuesSize];
                for (int i = 0; i < returnValuesSize; i++) {
                    returnValues[i] = Symbols.fromStream(in);
                }
            }
        } else {
            //Outputs should never be null and for BwC reasons
            //the default value in pre 4.1 was a long for a count
            outputs = new Symbol[]{new InputColumn(0, DataTypes.LONG)};
        }
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.SYS_UPDATE;
    }

    @Nullable
    public Symbol[] returnValues() {
        return returnValues;
    }

    @Override
    public List<? extends Symbol> outputs() {
        return List.of(outputs);
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitSysUpdateProjection(this, context);
    }

    public Map<Reference, Symbol> assignments() {
        return assignments;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(uidSymbol, out);
        out.writeVInt(assignments.size());
        for (Map.Entry<Reference, Symbol> e : assignments.entrySet()) {
            Reference.toStream(out, e.getKey());
            Symbols.toStream(e.getValue(), out);
        }

        if (out.getVersion().onOrAfter(Version.V_4_2_0)) {
            out.writeVInt(outputs.length);
            for (int i = 0; i < outputs.length; i++) {
                Symbols.toStream(outputs[i], out);
            }
            if (returnValues != null) {
                out.writeVInt(returnValues.length);
                for (int i = 0; i < returnValues.length; i++) {
                    Symbols.toStream(returnValues[i], out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SysUpdateProjection that = (SysUpdateProjection) o;
        return Objects.equals(uidSymbol, that.uidSymbol) &&
               Objects.equals(assignments, that.assignments) &&
               Arrays.equals(outputs, that.outputs) &&
               Arrays.equals(returnValues, that.returnValues);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(super.hashCode(), uidSymbol, assignments);
        result = 31 * result + Arrays.hashCode(outputs);
        result = 31 * result + Arrays.hashCode(returnValues);
        return result;
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.NODE;
    }
}

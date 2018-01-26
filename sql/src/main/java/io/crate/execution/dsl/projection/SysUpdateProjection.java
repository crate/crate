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

package io.crate.execution.dsl.projection;

import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.Reference;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class SysUpdateProjection extends DMLProjection {

    private Map<Reference, Symbol> assignments;

    public SysUpdateProjection(DataType idType, Map<Reference, Symbol> assignments) {
        super(new InputColumn(0, idType));
        this.assignments = assignments;
    }

    public SysUpdateProjection(StreamInput in) throws IOException {
        super(in);
        int numAssignments = in.readVInt();
        assignments = new HashMap<>(numAssignments, 1.0f);
        for (int i = 0; i < numAssignments; i++) {
            assignments.put(Reference.fromStream(in), Symbols.fromStream(in));
        }
    }

    @Override
    public void replaceSymbols(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        super.replaceSymbols(replaceFunction);
        if (assignments.isEmpty()) {
            return;
        }
        for (Map.Entry<Reference, Symbol> entry : assignments.entrySet()) {
            entry.setValue(replaceFunction.apply(entry.getValue()));
        }
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.SYS_UPDATE;
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
        super.writeTo(out);
        out.writeVInt(assignments.size());
        for (Map.Entry<Reference, Symbol> e : assignments.entrySet()) {
            Reference.toStream(e.getKey(), out);
            Symbols.toStream(e.getValue(), out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SysUpdateProjection that = (SysUpdateProjection) o;
        return Objects.equals(assignments, that.assignments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), assignments);
    }
}

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

package io.crate.planner.projection;

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.analyze.symbol.Value;
import io.crate.metadata.Reference;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.*;

public class SysUpdateProjection extends Projection {

    public static final ProjectionFactory<SysUpdateProjection> FACTORY = new ProjectionFactory<SysUpdateProjection>() {
        @Override
        public SysUpdateProjection newInstance() {
            return new SysUpdateProjection();
        }
    };
    private static final List<Value> OUTPUTS = Collections.singletonList(new Value(DataTypes.LONG));

    private Map<Reference, Symbol> assignments;

    private SysUpdateProjection() {
    }

    public SysUpdateProjection(Map<Reference, Symbol> assignments) {
        this.assignments = assignments;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.SYS_UPDATE;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitSysUpdateProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return OUTPUTS;
    }

    public Map<Reference, Symbol> assignments() {
        return assignments;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numAssignments = in.readVInt();
        assignments = new HashMap<>(numAssignments, 1.0f);
        for (int i = 0; i < numAssignments; i++) {
            assignments.put(Reference.fromStream(in), Symbols.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

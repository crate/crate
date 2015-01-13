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

import com.google.common.collect.ImmutableList;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Value;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UpdateProjection extends Projection {

    public static final ProjectionFactory<UpdateProjection> FACTORY = new ProjectionFactory<UpdateProjection>() {
        @Override
        public UpdateProjection newInstance() {
            return new UpdateProjection();
        }
    };

    protected final static List<Symbol> OUTPUTS = ImmutableList.<Symbol>of(
            new Value(DataTypes.LONG)  // number of rows updated
    );

    // The key of this map is expected to be a FQN columnIdent.
    private Map<String, Symbol> assignments;
    @Nullable
    private Long requiredVersion;
    private Symbol uidSymbol;

    public UpdateProjection(Symbol uidSymbol,
                            Map<String, Symbol> assignments,
                            @Nullable Long requiredVersion) {
        this.uidSymbol = uidSymbol;
        this.assignments = assignments;
        this.requiredVersion = requiredVersion;
    }

    public UpdateProjection() {
    }

    public Map<String, Symbol> assignments() {
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
        return OUTPUTS;
    }

    public Symbol uidSymbol() {
        return uidSymbol;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UpdateProjection that = (UpdateProjection) o;

        if (!assignments.equals(that.assignments)) return false;
        if (requiredVersion != null ? !requiredVersion.equals(that.requiredVersion) : that.requiredVersion != null)
            return false;
        if (!uidSymbol.equals(that.uidSymbol)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + assignments.hashCode();
        result = 31 * result + (requiredVersion != null ? requiredVersion.hashCode() : 0);
        result = 31 * result + uidSymbol.hashCode();
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        uidSymbol = Symbol.fromStream(in);
        int mapSize = in.readVInt();
        assignments = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            assignments.put(in.readString(), Symbol.fromStream(in));
        }
        requiredVersion = in.readVLong();
        if (requiredVersion == 0) {
            requiredVersion = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(uidSymbol, out);
        out.writeVInt(assignments.size());
        for (Map.Entry<String, Symbol> entry : assignments.entrySet()) {
            out.writeString(entry.getKey());
            Symbol.toStream(entry.getValue(), out);
        }
        if (requiredVersion == null) {
            out.writeVLong(0);
        } else {
            out.writeVLong(requiredVersion);
        }
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.SHARD;
    }
}

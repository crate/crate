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


import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class MergeProjection extends Projection {

    public static final ProjectionFactory<MergeProjection> FACTORY = new ProjectionFactory<MergeProjection>() {
        @Override
        public MergeProjection newInstance() {
            return new MergeProjection();
        }
    };

    private List<Symbol> outputs;

    private List<Symbol> orderBy;
    boolean[] reverseFlags;
    private Boolean[] nullsFirst;

    private MergeProjection() {
    }

    public MergeProjection(List<Symbol> outputs, List<Symbol> orderBy,
                           boolean[] reverseFlags, Boolean[] nullsFirst) {
        this.outputs = outputs;
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
        assert this.orderBy.size() == this.reverseFlags.length : "reverse flags length does not match orderBy items count";
        assert this.nullsFirst.length == this.reverseFlags.length;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.MERGE;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitMergeProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    public List<Symbol> orderBy() {
        return orderBy;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MergeProjection that = (MergeProjection) o;
        if (!Arrays.equals(reverseFlags, that.reverseFlags)) return false;
        if (!Arrays.equals(nullsFirst, that.nullsFirst)) return false;
        return Objects.equals(orderBy, that.orderBy);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int numOutputs = in.readVInt();
        outputs = new ArrayList<>(numOutputs);
        for (int i = 0; i < numOutputs; i++) {
            outputs.add(Symbol.fromStream(in));
        }
        int numOrderBy = in.readVInt();

        if (numOrderBy > 0) {
            reverseFlags = new boolean[numOrderBy];

            for (int i = 0; i < reverseFlags.length; i++) {
                reverseFlags[i] = in.readBoolean();
            }

            orderBy = new ArrayList<>(numOrderBy);
            for (int i = 0; i < reverseFlags.length; i++) {
                orderBy.add(Symbol.fromStream(in));
            }

            nullsFirst = new Boolean[numOrderBy];
            for (int i = 0; i < numOrderBy; i++) {
                nullsFirst[i] = in.readOptionalBoolean();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(outputs.size());
        for (Symbol symbol : outputs) {
            Symbol.toStream(symbol, out);
        }
        if (isOrdered()) {
            out.writeVInt(reverseFlags.length);
            for (boolean reverseFlag : reverseFlags) {
                out.writeBoolean(reverseFlag);
            }
            for (Symbol symbol : orderBy) {
                Symbol.toStream(symbol, out);
            }
            for (Boolean nullFirst : nullsFirst) {
                out.writeOptionalBoolean(nullFirst);
            }
        } else {
            out.writeVInt(0);
        }

    }

    public boolean isOrdered() {
        return reverseFlags != null && reverseFlags.length > 0;
    }
}

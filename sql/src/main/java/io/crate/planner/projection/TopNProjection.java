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

package io.crate.planner.projection;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TopNProjection extends Projection {

    public static final ProjectionFactory<TopNProjection> FACTORY = new ProjectionFactory<TopNProjection>() {
        @Override
        public TopNProjection newInstance() {
            return new TopNProjection();
        }
    };


    private int limit;
    private int offset;

    List<Symbol> outputs = ImmutableList.of();

    List<Symbol> orderBy;
    boolean[] reverseFlags;
    private Boolean[] nullsFirst;

    public TopNProjection() {
        super();
    }

    public TopNProjection(int limit, int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    public TopNProjection(int limit, int offset, @Nullable List<Symbol> orderBy, @Nullable boolean[] reverseFlags, @Nullable Boolean[] nullsFirst) {
        this(limit, offset);
        this.orderBy = Objects.firstNonNull(orderBy, ImmutableList.<Symbol>of());
        if (this.orderBy.isEmpty()) {
            this.reverseFlags = new boolean[0];
            this.nullsFirst = new Boolean[0];
        } else {
            this.reverseFlags = Objects.firstNonNull(reverseFlags, new boolean[0]);
            this.nullsFirst = Objects.firstNonNull(nullsFirst, new Boolean[0]);
        }
        assert this.orderBy.size() == this.reverseFlags.length : "reverse flags length does not match orderBy items count";
        assert this.nullsFirst.length == this.reverseFlags.length;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public void outputs(List<Symbol> outputs) {
        this.outputs = outputs;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
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

    public boolean isOrdered() {
        return reverseFlags != null && reverseFlags.length > 0;
    }


    @Override
    public ProjectionType projectionType() {
        return ProjectionType.TOPN;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitTopNProjection(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        offset = in.readVInt();
        limit = in.readVInt();

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
        out.writeVInt(offset);
        out.writeVInt(limit);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopNProjection that = (TopNProjection) o;

        if (limit != that.limit) return false;
        if (offset != that.offset) return false;
        if (!orderBy.equals(that.orderBy)) return false;
        if (!outputs.equals(that.outputs)) return false;
        if (!Arrays.equals(reverseFlags, that.reverseFlags)) return false;
        if (!Arrays.equals(nullsFirst, that.nullsFirst)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = limit;
        result = 31 * result + offset;
        result = 31 * result + outputs.hashCode();
        result = 31 * result + orderBy.hashCode();
        result = 31 * result + Arrays.hashCode(reverseFlags);
        result = 31 * result + Arrays.hashCode(nullsFirst);
        return result;
    }

    @Override
    public String toString() {
        return "TopNProjection{" +
                "outputs=" + outputs +
                ", limit=" + limit +
                ", offset=" + offset +
                ", orderBy=" + orderBy +
                ", reverseFlags=" + Arrays.toString(reverseFlags) +
                ", nullsFirst=" + Arrays.toString(nullsFirst) +
                '}';
    }
}

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

import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupProjection extends Projection {

    List<Symbol> keys;
    List<Aggregation> values;

    public static final ProjectionFactory<GroupProjection> FACTORY = new ProjectionFactory<GroupProjection>() {
        @Override
        public GroupProjection newInstance() {
            return new GroupProjection();
        }
    };


    public List<Symbol> keys() {
        return keys;
    }

    public void keys(List<Symbol> keys) {
        this.keys = keys;
    }

    public List<Aggregation> values() {
        return values;
    }

    public void values(List<Aggregation> values) {
        this.values = values;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.GROUP;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitGroupProjection(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        keys = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            keys.add(Symbol.fromStream(in));
        }
        size = in.readVInt();
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add((Aggregation) Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(keys.size());
        for (Symbol symbol : keys) {
            Symbol.toStream(symbol, out);
        }

        out.writeVInt(values.size());
        for (Symbol symbol : values) {
            Symbol.toStream(symbol, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GroupProjection that = (GroupProjection) o;

        if (!keys.equals(that.keys)) return false;
        if (values != null ? !values.equals(that.values) : that.values != null) return false;

        return true;
    }

}

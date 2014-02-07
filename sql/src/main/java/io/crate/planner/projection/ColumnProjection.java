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

import com.google.common.collect.ImmutableList;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnProjection extends Projection {

    List<Symbol> outputs;

    public static final ProjectionFactory<ColumnProjection> FACTORY = new ProjectionFactory<ColumnProjection>() {
        @Override
        public ColumnProjection newInstance() {
            return new ColumnProjection();
        }
    };


    @Override
    public ImmutableList<Symbol> outputs() {
        return ImmutableList.copyOf(outputs);
    }

    public void outputs(List<Symbol> outputs) {
        this.outputs = outputs;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.COLUMN;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitColumnProjection(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        outputs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            outputs.add(Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(outputs.size());
        for (Symbol symbol : outputs) {
            Symbol.toStream(symbol, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnProjection that = (ColumnProjection) o;

        if (!outputs.equals(that.outputs)) return false;

        return true;
    }

    @Override
    public String toString() {
        return "ColumnProjection{" +
                "outputs=" + outputs +
                '}';
    }

}

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

import com.google.common.base.Function;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupProjection extends Projection {

    private List<Symbol> keys;
    private List<Aggregation> values;
    private List<Symbol> outputs;

    private RowGranularity requiredGranularity = RowGranularity.CLUSTER;

    public static final ProjectionFactory<GroupProjection> FACTORY = new ProjectionFactory<GroupProjection>() {
        @Override
        public GroupProjection newInstance() {
            return new GroupProjection();
        }
    };

    private GroupProjection() {
    }

    public GroupProjection(List<Symbol> keys, List<Aggregation> values, RowGranularity requiredGranularity) {
        this.keys = keys;
        this.values = values;
        this.requiredGranularity = requiredGranularity;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        Lists2.replaceItems(keys, replaceFunction);
        Lists2.replaceItems(outputs, replaceFunction);
    }

    public List<Symbol> keys() {
        return keys;
    }

    public List<Aggregation> values() {
        return values;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.GROUP;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitGroupProjection(this, context);
    }

    /**
     * returns a list of outputs, with the group by keys going first,
     * and the aggregations coming last
     */
    @Override
    public List<? extends Symbol> outputs() {
        if (outputs == null) {
            outputs = new ArrayList<>(keys.size() + values.size());
            outputs.addAll(keys);
            outputs.addAll(values);
        }
        return outputs;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        keys = Symbols.listFromStream(in);
        int size = in.readVInt();
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add((Aggregation) Symbols.fromStream(in));
        }
        requiredGranularity = RowGranularity.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(keys, out);
        Symbols.toStream(values, out);
        RowGranularity.toStream(requiredGranularity, out);
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

    @Override
    public RowGranularity requiredGranularity() {
        return requiredGranularity;
    }
}

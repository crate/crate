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

package io.crate.execution.dsl.projection;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class GroupProjection extends Projection {

    private List<Symbol> keys;
    private List<Aggregation> values;
    private List<Symbol> outputs;

    private AggregateMode mode;
    private RowGranularity requiredGranularity = RowGranularity.CLUSTER;

    public GroupProjection(List<Symbol> keys,
                           List<Aggregation> values,
                           AggregateMode mode,
                           RowGranularity requiredGranularity) {
        this.keys = keys;
        this.values = values;
        this.mode = mode;
        this.requiredGranularity = requiredGranularity;
    }

    public GroupProjection(StreamInput in) throws IOException {
        mode = AggregateMode.readFrom(in);
        keys = Symbols.listFromStream(in);
        int size = in.readVInt();
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add((Aggregation) Symbols.fromStream(in));
        }
        requiredGranularity = RowGranularity.fromStream(in);
    }

    @Override
    public void replaceSymbols(Function<? super Symbol, ? extends Symbol> replaceFunction) {
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
    public void writeTo(StreamOutput out) throws IOException {
        AggregateMode.writeTo(mode, out);
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

    public AggregateMode mode() {
        return mode;
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "HashAggregation",
            "keys", ExplainLeaf.printList(keys),
            "aggregations", ExplainLeaf.printList(values)
        );
    }
}

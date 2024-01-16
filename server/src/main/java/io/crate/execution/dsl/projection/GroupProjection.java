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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Aggregation;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;

public class GroupProjection extends Projection {

    private final List<Symbol> keys;
    private final List<Aggregation> values;
    private List<Symbol> outputs;

    private final AggregateMode mode;
    private final RowGranularity requiredGranularity;

    public GroupProjection(List<Symbol> keys,
                           List<Aggregation> values,
                           AggregateMode mode,
                           RowGranularity requiredGranularity) {
        assert keys.stream().noneMatch(s ->
            SymbolVisitors.any(Symbols.IS_COLUMN.or(x -> x instanceof SelectSymbol), s))
            : "Cannot operate on Reference, Field or SelectSymbol symbols: " + keys;
        assert values.stream().noneMatch(s ->
            SymbolVisitors.any(Symbols.IS_COLUMN.or(x -> x instanceof SelectSymbol), s))
            : "Cannot operate on Reference, Field or SelectSymbol symbols: " + values;
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
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupProjection that = (GroupProjection) o;
        return Objects.equals(keys, that.keys) &&
               Objects.equals(values, that.values) &&
               Objects.equals(outputs, that.outputs) &&
               mode == that.mode &&
               requiredGranularity == that.requiredGranularity;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keys, values, outputs, mode, requiredGranularity);
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
        return MapBuilder.<String, Object>newMapBuilder()
            .put("type", "HashAggregation")
            .put("keys", Lists.joinOn(", ", keys, Symbol::toString))
            .put("aggregations", Lists.joinOn(", ", values, Symbol::toString))
            .map();
    }
}

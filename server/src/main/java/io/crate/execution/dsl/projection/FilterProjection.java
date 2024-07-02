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
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.MapBuilder;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;

public class FilterProjection extends Projection {

    private final Symbol query;
    private final List<Symbol> outputs;
    private RowGranularity requiredGranularity = RowGranularity.CLUSTER;

    public FilterProjection(Symbol query, List<Symbol> outputs) {
        assert !query.any(Symbol.IS_COLUMN.or(s -> s instanceof SelectSymbol))
            : "FilterProjection cannot operate on Reference, Field or SelectSymbol symbols: " + query;
        assert outputs.stream().noneMatch(s ->
            s.any(Symbol.IS_COLUMN.or(x -> x instanceof SelectSymbol)))
            : "Cannot operate on Reference, Field or SelectSymbol symbols: " + outputs;
        this.query = query;
        this.outputs = outputs;
    }

    public FilterProjection(StreamInput in) throws IOException {
        query = Symbol.fromStream(in);
        outputs = Symbols.fromStream(in);
        requiredGranularity = RowGranularity.fromStream(in);
    }

    @Override
    public RowGranularity requiredGranularity() {
        return requiredGranularity;
    }

    public void requiredGranularity(RowGranularity requiredRowGranularity) {
        this.requiredGranularity = requiredRowGranularity;
    }

    public Symbol query() {
        return query;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.FILTER;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitFilterProjection(this, context);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FilterProjection that = (FilterProjection) o;

        return Objects.equals(query, that.query);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(query, out);
        Symbols.toStream(outputs, out);
        RowGranularity.toStream(requiredGranularity, out);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return 31 * result + (query != null ? query.hashCode() : 0);
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return MapBuilder.<String, Object>newMapBuilder()
            .put("type", "Filter")
            .put("filter", query.toString())
            .map();
    }
}

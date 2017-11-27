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
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.symbol.AggregateMode;
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.metadata.RowGranularity;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A projection which aggregates all inputs to a single row
 */
public class AggregationProjection extends Projection {

    private RowGranularity contextGranularity;
    private AggregateMode mode;
    private List<Aggregation> aggregations = ImmutableList.of();

    public AggregationProjection(StreamInput in) throws IOException {
        int size = in.readVInt();
        aggregations = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            aggregations.add((Aggregation) Symbols.fromStream(in));
        }
        contextGranularity = RowGranularity.fromStream(in);
        mode = AggregateMode.readFrom(in);
    }

    public AggregationProjection(List<Aggregation> aggregations, RowGranularity contextGranularity, AggregateMode mode) {
        assert aggregations != null : "aggregations must not be null";

        this.contextGranularity = contextGranularity;
        this.mode = mode;
        this.aggregations = aggregations;
    }

    @Override
    public RowGranularity requiredGranularity() {
        return contextGranularity;
    }

    @Override
    public void replaceSymbols(Function<? super Symbol, ? extends Symbol> replaceFunction) {
    }

    public List<Aggregation> aggregations() {
        return aggregations;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.AGGREGATION;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitAggregationProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return aggregations;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(aggregations, out);
        RowGranularity.toStream(contextGranularity, out);
        AggregateMode.writeTo(mode, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregationProjection that = (AggregationProjection) o;
        if (aggregations != null ? !aggregations.equals(that.aggregations) : that.aggregations != null) return false;

        return true;
    }

    public AggregateMode mode() {
        return mode;
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "HashAggregation",
            "aggregations", ExplainLeaf.printList(aggregations)
        );
    }
}

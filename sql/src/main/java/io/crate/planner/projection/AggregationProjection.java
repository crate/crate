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
import io.crate.analyze.symbol.Aggregation;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A projection which aggregates all inputs to a single row
 */
public class AggregationProjection extends Projection {

    List<Aggregation> aggregations = ImmutableList.of();

    public static final ProjectionFactory<AggregationProjection> FACTORY = new ProjectionFactory<AggregationProjection>() {
        @Override
        public AggregationProjection newInstance() {
            return new AggregationProjection();
        }
    };

    public AggregationProjection() {
    }

    public AggregationProjection(List<Aggregation> aggregations) {
        assert aggregations != null;
        this.aggregations = aggregations;
    }

    @Override
    public RowGranularity requiredGranularity() {
        return RowGranularity.SHARD;
    }

    public List<Aggregation> aggregations() {
        return aggregations;
    }

    public void aggregations(List<Aggregation> aggregations) {
        assert aggregations != null;
        this.aggregations = aggregations;
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
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        aggregations = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            aggregations.add((Aggregation) Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(aggregations, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AggregationProjection that = (AggregationProjection) o;
        if (aggregations != null ? !aggregations.equals(that.aggregations) : that.aggregations != null) return false;

        return true;
    }

}

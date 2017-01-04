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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.RowGranularity;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class FilterProjection extends Projection {

    private Symbol query;
    private List<Symbol> outputs = ImmutableList.of();
    private RowGranularity requiredGranularity = RowGranularity.CLUSTER;

    @Override
    public RowGranularity requiredGranularity() {
        return requiredGranularity;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        query = replaceFunction.apply(query);
        Lists2.replaceItems(outputs, replaceFunction);
    }

    public void requiredGranularity(RowGranularity requiredRowGranularity) {
        this.requiredGranularity = requiredRowGranularity;
    }

    public FilterProjection(Symbol query) {
        this.query = query;
    }

    public FilterProjection(Symbol query, List<Symbol> outputs) {
        this(query);
        this.outputs = outputs;
    }

    public FilterProjection(StreamInput in) throws IOException {
        query = Symbols.fromStream(in);
        outputs = Symbols.listFromStream(in);
        requiredGranularity = RowGranularity.fromStream(in);
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
        Symbols.toStream(query, out);
        Symbols.toStream(outputs, out);
        RowGranularity.toStream(requiredGranularity, out);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return 31 * result + (query != null ? query.hashCode() : 0);
    }
}

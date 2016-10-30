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

public class FilterProjection extends Projection {

    public static final ProjectionFactory<FilterProjection> FACTORY = new ProjectionFactory<FilterProjection>() {
        @Override
        public FilterProjection newInstance() {
            return new FilterProjection();
        }
    };

    private Symbol query;
    private List<Symbol> outputs = ImmutableList.of();
    private RowGranularity requiredGranularity = RowGranularity.CLUSTER;

    public FilterProjection() {
    }

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
        outputs(outputs);
        this.query = query;
    }


    public void query(Symbol query) {
        this.query = query;
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

    public void outputs(List<Symbol> outputs) {
        this.outputs = outputs;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        query = Symbols.fromStream(in);
        outputs = Symbols.listFromStream(in);
        requiredGranularity = RowGranularity.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(query, out);
        Symbols.toStream(outputs, out);
        RowGranularity.toStream(requiredGranularity, out);
    }
}

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

package io.crate.planner.plan;

// PRESTOBORROW


import com.google.common.collect.ImmutableList;
import io.crate.planner.projections.Projection;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Deprecated
public abstract class PlanNode implements Streamable {

    private String id;
    private List<PlanNode> sources;
    private List<Symbol> outputs;
    private List<Projection> projections = new ArrayList<>();

    protected PlanNode() {

    }

    protected PlanNode(String id) {
        this.id = id;
    }

    protected PlanNode(String id, List<PlanNode> sources) {
        this(id);
        this.sources = sources;
    }

    public String id() {
        return id;
    }

    public void source(PlanNode source) {
        this.sources = ImmutableList.of(source);
    }

    public void sources(List<PlanNode> sources) {
        this.sources = sources;
    }

    public List<PlanNode> sources() {
        return sources;
    }

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitPlan(this, context);
    }

    public void outputs(Symbol... outputs) {
        this.outputs = ImmutableList.copyOf(outputs);
    }

    public String getId() {
        return id;
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    public List<Projection> projections() {
        return projections;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readString();

        int numOutputs = in.readVInt();
        outputs = new ArrayList<>(numOutputs);
        for (int i = 0; i < numOutputs; i++) {
            outputs.add(Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);

        out.writeVInt(outputs.size());
        for (Symbol output : outputs) {
            Symbol.toStream(output, out);
        }
    }
}

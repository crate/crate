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

package io.crate.planner.node;

import com.google.common.collect.ImmutableSet;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by bd on 5.2.14.
 */
public abstract class AbstractESNode extends PlanNode {

    protected List<Symbol> outputs;

    public List<Symbol> outputs() {
        return outputs;
    }

    public void outputs(List<Symbol> outputs) {
        this.outputs = outputs;
    }

    @Override
    public Set<String> executionNodes() {
        return ImmutableSet.of();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numOutputs = in.readVInt();
        outputs = new ArrayList<>(numOutputs);
        for (int i = 0; i < numOutputs; i++) {
            outputs.add(Symbol.fromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(outputs.size());
        for (Symbol output : outputs) {
            Symbol.toStream(output, out);
        }
    }
}

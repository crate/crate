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

import io.crate.planner.symbol.Symbol;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A plan node which collects data.
 */
public class CollectNode extends PlanNode {

    private List<Symbol> toCollect;

    protected CollectNode() {
    }

    public List<Symbol> toCollect() {
        return toCollect;
    }

    public void toCollect(List<Symbol> toCollect) {
        this.toCollect = toCollect;
    }

    @Override
    public List<DataType> outputTypes() {
        // TODO: impl
        return null;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitCollectNode(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        int numCols = in.readVInt();
        if (numCols > 0) {
            toCollect = new ArrayList<>(numCols);
            for (int i = 0; i < numCols; i++) {
                toCollect.add(Symbol.fromStream(in));
            }
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        int numCols = toCollect.size();
        out.writeVInt(numCols);
        for (int i = 0; i < numCols; i++) {
            Symbol.toStream(toCollect.get(i), out);
        }
    }

}

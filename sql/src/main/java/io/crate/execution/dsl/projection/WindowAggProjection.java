/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dsl.projection;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WindowDefinition;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class WindowAggProjection extends Projection {

    private final WindowDefinition windowDefinition;
    private final List<Symbol> standaloneWithInputs;
    private LinkedHashMap<WindowFunction, List<Symbol>> functionsWithInputs;
    private ArrayList<Symbol> outputs;
    @Nullable
    private int[] orderByIndexes;

    public WindowAggProjection(WindowDefinition windowDefinition,
                               LinkedHashMap<WindowFunction, List<Symbol>> functionsWithInputs,
                               List<Symbol> standaloneWithInputs,
                               @Nullable int[] orderByIndexes) {

        Set<WindowFunction> windowFunctions = functionsWithInputs.keySet();
        assert windowFunctions.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + windowFunctions;
        assert standaloneWithInputs.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + standaloneWithInputs;
        this.windowDefinition = windowDefinition;
        this.functionsWithInputs = functionsWithInputs;
        this.standaloneWithInputs = standaloneWithInputs;
        this.orderByIndexes = orderByIndexes;
        outputs = new ArrayList<>(windowFunctions);
        outputs.addAll(standaloneWithInputs);
    }

    public WindowAggProjection(StreamInput in) throws IOException {
        windowDefinition = new WindowDefinition(in);
        standaloneWithInputs = Symbols.listFromStream(in);
        int functionsCount = in.readVInt();
        functionsWithInputs = new LinkedHashMap<>(functionsCount, 1f);
        for (int i = 0; i < functionsCount; i++) {
            WindowFunction function = (WindowFunction) Symbols.fromStream(in);
            List<Symbol> inputs = Symbols.listFromStream(in);
            functionsWithInputs.put(function, inputs);
        }
        if (in.readBoolean()) {
            orderByIndexes = in.readIntArray();
        }
        outputs = new ArrayList<>(functionsWithInputs.keySet());
    }

    public WindowDefinition windowDefinition() {
        return windowDefinition;
    }

    public LinkedHashMap<WindowFunction, List<Symbol>> functionsWithInputs() {
        return functionsWithInputs;
    }

    public List<Symbol> standalone() {
        return standaloneWithInputs;
    }

    @Nullable
    public int[] orderByIndexes() {
        return orderByIndexes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowAggProjection that = (WindowAggProjection) o;
        return Objects.equals(windowDefinition, that.windowDefinition) &&
               Objects.equals(functionsWithInputs, that.functionsWithInputs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowDefinition, functionsWithInputs);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.WINDOW_AGGREGATION;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitWindowAgg(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        windowDefinition.writeTo(out);
        Symbols.toStream(standaloneWithInputs, out);
        out.writeVInt(functionsWithInputs.size());
        for (Map.Entry<WindowFunction, List<Symbol>> functionWithInputs : functionsWithInputs.entrySet()) {
            Symbols.toStream(functionWithInputs.getKey(), out);
            Symbols.toStream(functionWithInputs.getValue(), out);
        }
        if (orderByIndexes != null) {
            out.writeBoolean(true);
            out.writeIntArray(orderByIndexes);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "WindowAggregation",
            "windowFunctions",
            ExplainLeaf.printList(new ArrayList<>(functionsWithInputs.keySet()))
        );
    }
}

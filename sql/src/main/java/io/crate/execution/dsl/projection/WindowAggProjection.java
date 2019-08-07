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

import io.crate.analyze.WindowDefinition;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;
import io.crate.expression.symbol.WindowFunctionContext;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class WindowAggProjection extends Projection {

    private final WindowDefinition windowDefinition;
    private final List<Symbol> standaloneWithInputs;
    private final ArrayList<Symbol> outputs;
    private final ArrayList<WindowFunctionContext> windowFunctionContexts;
    private final ArrayList<WindowFunction> windowFunctions;

    public WindowAggProjection(WindowDefinition windowDefinition,
                               ArrayList<WindowFunctionContext> windowFunctionContexts,
                               List<Symbol> standaloneWithInputs) {
        this.windowFunctions = windowFunctionContexts.stream()
            .map(WindowFunctionContext::function)
            .collect(Collectors.toCollection(ArrayList::new));
        assert windowFunctions.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + windowFunctions;
        assert standaloneWithInputs.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + standaloneWithInputs;
        this.windowDefinition = windowDefinition;
        this.windowFunctionContexts = windowFunctionContexts;
        this.standaloneWithInputs = standaloneWithInputs;
        outputs = new ArrayList<>(standaloneWithInputs);
        outputs.addAll(windowFunctions);
    }

    WindowAggProjection(StreamInput in) throws IOException {
        windowDefinition = new WindowDefinition(in);
        standaloneWithInputs = Symbols.listFromStream(in);
        int functionsCount = in.readVInt();
        windowFunctionContexts = new ArrayList<>(functionsCount);
        windowFunctions = new ArrayList<>(functionsCount);
        boolean onOrAfter4_1_0 = in.getVersion().onOrAfter(Version.V_4_1_0);
        for (int i = 0; i < functionsCount; i++) {
            WindowFunction function = (WindowFunction) Symbols.fromStream(in);
            Symbol filter;
            if (onOrAfter4_1_0) {
                filter = Symbols.fromStream(in);
            } else {
                filter = Literal.BOOLEAN_TRUE;
            }
            List<Symbol> inputs = Symbols.listFromStream(in);
            windowFunctionContexts.add(
                new WindowFunctionContext(function, inputs, filter));
            windowFunctions.add(function);
        }
        outputs = new ArrayList<>(standaloneWithInputs);
        outputs.addAll(windowFunctions);
    }

    public WindowDefinition windowDefinition() {
        return windowDefinition;
    }

    public List<WindowFunctionContext> windowFunctionContexts() {
        return windowFunctionContexts;
    }

    public List<Symbol> standalone() {
        return standaloneWithInputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowAggProjection that = (WindowAggProjection) o;
        return Objects.equals(windowDefinition, that.windowDefinition) &&
               Objects.equals(windowFunctionContexts, that.windowFunctionContexts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowDefinition, windowFunctionContexts);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.WINDOW_AGGREGATION;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitWindowAgg(this, context);
    }

    /**
     * Returns the output symbols list in a defined order.
     *
     * The standalone input symbols placed first followed by the
     * window function symbols.
     */
    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        windowDefinition.writeTo(out);
        Symbols.toStream(standaloneWithInputs, out);
        out.writeVInt(windowFunctionContexts.size());
        boolean onOrAfter4_1_0 = out.getVersion().onOrAfter(Version.V_4_1_0);
        for (var windowFunctionContext : windowFunctionContexts) {
            Symbols.toStream(windowFunctionContext.function(), out);
            if (onOrAfter4_1_0) {
                Symbols.toStream(windowFunctionContext.filter(), out);
            }
            Symbols.toStream(windowFunctionContext.inputs(), out);
        }
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return Map.of(
            "type", "WindowAggregation",
            "windowFunctions", ExplainLeaf.printList(windowFunctions)
        );
    }
}

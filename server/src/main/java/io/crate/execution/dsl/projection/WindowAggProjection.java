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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.analyze.WindowDefinition;
import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.expression.symbol.WindowFunction;

public class WindowAggProjection extends Projection {

    private final WindowDefinition windowDefinition;
    private final List<Symbol> standaloneWithInputs;
    private final ArrayList<Symbol> outputs;
    private final List<WindowFunction> windowFunctions;

    public WindowAggProjection(WindowDefinition windowDefinition,
                               List<WindowFunction> windowFunctions,
                               List<Symbol> standaloneWithInputs) {
        this.windowFunctions = windowFunctions;
        assert windowFunctions.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + windowFunctions;
        assert standaloneWithInputs.stream().noneMatch(Symbols.IS_COLUMN)
            : "Cannot operate on Reference or Field: " + standaloneWithInputs;
        this.windowDefinition = windowDefinition;
        this.standaloneWithInputs = standaloneWithInputs;
        outputs = new ArrayList<>(standaloneWithInputs);
        outputs.addAll(windowFunctions);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    WindowAggProjection(StreamInput in) throws IOException {
        windowDefinition = new WindowDefinition(in);
        standaloneWithInputs = Symbols.listFromStream(in);

        Version version = in.getVersion();
        if (version.onOrAfter(Version.V_4_2_1)) {
            windowFunctions = (List<WindowFunction>) (List)Symbols.listFromStream(in);
        } else {
            boolean onOrAfter4_1_0 = version.onOrAfter(Version.V_4_1_0);
            int functionsCount = in.readVInt();
            windowFunctions = new ArrayList<>();
            for (int i = 0; i < functionsCount; i++) {
                WindowFunction function = (WindowFunction) Symbols.fromStream(in);
                if (onOrAfter4_1_0) {
                    // used to be the filter
                    Symbols.fromStream(in);
                }
                // used to be the inputs (arguments of the window function)
                Symbols.listFromStream(in);
                windowFunctions.add(function);
            }
        }
        outputs = new ArrayList<>(standaloneWithInputs);
        outputs.addAll(windowFunctions);
    }

    public WindowDefinition windowDefinition() {
        return windowDefinition;
    }

    public List<WindowFunction> windowFunctions() {
        return windowFunctions;
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
               Objects.equals(windowFunctions, that.windowFunctions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowDefinition, windowFunctions);
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

        Version version = out.getVersion();
        if (version.onOrAfter(Version.V_4_2_1)) {
            Symbols.toStream(windowFunctions, out);
        } else {
            boolean onOrAfter4_1_0 = version.onOrAfter(Version.V_4_1_0);
            out.writeVInt(windowFunctions.size());
            for (var windowFunction : windowFunctions) {
                Symbols.toStream(windowFunction, out);
                if (onOrAfter4_1_0) {
                    Symbol filter = windowFunction.filter();
                    Symbols.toStream(filter == null ? Literal.BOOLEAN_TRUE : filter, out);
                }
                Symbols.toStream(windowFunction.arguments(), out);
            }
        }
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return MapBuilder.<String, Object>newMapBuilder()
            .put("type", "WindowAggregation")
            .put("windowFunctions", Lists.joinOn(", ", windowFunctions, Symbol::toString))
            .map();
    }
}

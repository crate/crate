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
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class WindowAggProjection extends Projection {

    private final Map<WindowDefinition, List<Symbol>> functionsByWindow;

    public WindowAggProjection(Map<WindowDefinition, List<Symbol>> functionsByWindow) {
        this.functionsByWindow = functionsByWindow;
    }

    public WindowAggProjection(StreamInput in) throws IOException {
        int size = in.readVInt();
        functionsByWindow = new HashMap<>(size, 1f);
        for (int i = 0; i < size; i++) {
            WindowDefinition windowDefinition = new WindowDefinition(in);
            functionsByWindow.put(windowDefinition, Symbols.listFromStream(in));
        }
    }

    public Map<WindowDefinition, List<Symbol>> functionsByWindow() {
        return functionsByWindow;
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
        return functionsByWindow.values().stream()
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(functionsByWindow.size());
        for (Map.Entry<WindowDefinition, List<Symbol>> entry : functionsByWindow.entrySet()) {
            entry.getKey().writeTo(out);
            Symbols.toStream(entry.getValue(), out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowAggProjection that = (WindowAggProjection) o;
        return Objects.equals(functionsByWindow, that.functionsByWindow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), functionsByWindow);
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "WindowAggregation",
            "windowFunctions", ExplainLeaf.printList(
                functionsByWindow.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList())
            )
        );
    }
}

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
import io.crate.collections.Lists2;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitors;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Projection which can evaluate functions or re-order columns
 */
public class EvalProjection extends Projection {

    private final List<Symbol> outputs;

    public EvalProjection(List<Symbol> outputs) {
        assert outputs.stream().noneMatch(s -> SymbolVisitors.any(Symbols.IS_COLUMN, s))
            : "EvalProjection doesn't support Field or Reference symbols, got: " + outputs;
        this.outputs = outputs;
    }

    public EvalProjection(StreamInput in) throws IOException {
        this.outputs = Symbols.listFromStream(in);
    }

    @Override
    public void replaceSymbols(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        Lists2.replaceItems(outputs, replaceFunction);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.EVAL;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitEvalProjection(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbols.toStream(outputs, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EvalProjection that = (EvalProjection) o;

        return outputs.equals(that.outputs);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + outputs.hashCode();
        return result;
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "Eval",
            "outputs", ExplainLeaf.printList(outputs)
        );
    }
}

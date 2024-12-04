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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

/**
 * Projection which can evaluate functions or re-order columns
 */
public class EvalProjection extends Projection {

    private final List<Symbol> outputs;
    private final RowGranularity granularity;

    @Nullable
    public static EvalProjection castValues(List<DataType<?>> targetTypes, List<Symbol> sources) {
        ArrayList<Symbol> casts = new ArrayList<>(targetTypes.size());
        boolean requiresCasts = false;
        for (int i = 0; i < sources.size(); i++) {
            Symbol source = sources.get(i);
            DataType<?> targetType = targetTypes.get(i);
            InputColumn inputColumn = new InputColumn(i, source.valueType());
            if (targetType.id() == DataTypes.UNDEFINED.id() || targetType.equals(source.valueType())) {
                casts.add(inputColumn);
            } else {
                requiresCasts = true;
                casts.add(inputColumn.cast(targetType));
            }
        }
        return requiresCasts ? new EvalProjection(casts) : null;
    }

    public EvalProjection(List<Symbol> outputs) {
        this(outputs, RowGranularity.CLUSTER);
    }

    public EvalProjection(List<Symbol> outputs, RowGranularity granularity) {
        assert outputs.stream().noneMatch(
            s -> s.any(Symbol.IS_COLUMN.or(x -> x instanceof SelectSymbol)))
            : "EvalProjection doesn't support Field, Reference or SelectSymbol symbols, got: " + outputs;
        this.outputs = outputs;
        this.granularity = granularity;
    }

    public EvalProjection(StreamInput in) throws IOException {
        this.outputs = Symbols.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_4_5_3)) {
            this.granularity = RowGranularity.fromStream(in) ;
        } else {
            this.granularity = RowGranularity.CLUSTER;
        }
    }

    @Override
    public RowGranularity requiredGranularity() {
        return this.granularity;
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
        if (out.getVersion().onOrAfter(Version.V_4_5_3)) {
            RowGranularity.toStream(granularity, out);
        }
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
        return MapBuilder.<String, Object>newMapBuilder()
            .put("type", "Eval")
            .put("outputs", Lists.joinOn(", ", outputs, Symbol::toString))
            .map();
    }

    @Override
    public String toString() {
        return "EvalProjection{" +
               "outputs=" + outputs +
               '}';
    }
}

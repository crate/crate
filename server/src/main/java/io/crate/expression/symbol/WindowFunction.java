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

package io.crate.expression.symbol;

import static io.crate.metadata.FunctionType.AGGREGATE;
import static io.crate.metadata.FunctionType.WINDOW;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.common.collections.Lists;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;

public class WindowFunction extends Function {

    private final WindowDefinition windowDefinition;
    @Nullable
    private final Boolean ignoreNulls;

    public WindowFunction(StreamInput in) throws IOException {
        super(in);
        windowDefinition = new WindowDefinition(in);
        if (in.getVersion().onOrAfter(Version.V_4_7_0)) {
            ignoreNulls = in.readOptionalBoolean();
        } else {
            ignoreNulls = null;
        }
    }

    public WindowFunction(Signature signature,
                          List<Symbol> arguments,
                          DataType<?> returnType,
                          @Nullable Symbol filter,
                          WindowDefinition windowDefinition,
                          @Nullable Boolean ignoreNulls) {
        super(signature, arguments, returnType, filter);
        assert signature.getKind() == WINDOW || signature.getKind() == AGGREGATE :
            "only window and aggregate functions are allowed to be modelled over a window";
        this.windowDefinition = windowDefinition;
        this.ignoreNulls = ignoreNulls;
    }

    public WindowDefinition windowDefinition() {
        return windowDefinition;
    }

    @Nullable
    public Boolean ignoreNulls() {
        return ignoreNulls;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitWindowFunction(this, context);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.WINDOW_FUNCTION;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        windowDefinition.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_4_7_0)) {
            out.writeOptionalBoolean(ignoreNulls);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        WindowFunction that = (WindowFunction) o;
        return Objects.equals(ignoreNulls, that.ignoreNulls) && windowDefinition.equals(that.windowDefinition);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + windowDefinition.hashCode();
        result = 31 * result + (ignoreNulls == null ? 0 : ignoreNulls.hashCode());
        return result;
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        var builder = new StringBuilder(super.toString(style));
        if (ignoreNulls != null) {
            if (ignoreNulls) {
                builder.append(" IGNORE NULLS");
            } else {
                builder.append(" RESPECT NULLS");
            }
        }
        builder.append(" OVER (");

        var partitions = windowDefinition.partitions();
        if (!partitions.isEmpty()) {
            builder.append("PARTITION BY ");
            builder.append(Lists.joinOn(", ", partitions, x -> x.toString(style)));
        }
        var orderBy = windowDefinition.orderBy();
        if (orderBy != null) {
            if (!partitions.isEmpty()) {
                builder.append(" ");
            }
            builder.append("ORDER BY ");
            OrderBy.explainRepresentation(
                builder,
                orderBy.orderBySymbols(),
                orderBy.reverseFlags(),
                orderBy.nullsFirst(),
                x -> x.toString(style)
            );
        }
        WindowFrameDefinition frameDefinition = windowDefinition.windowFrameDefinition();
        if (frameDefinition != WindowDefinition.RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW) {
            builder.append(" ");
            builder.append(frameDefinition.mode().name());
            builder.append(" BETWEEN ");
            appendFrameBound(builder, style, frameDefinition.start());
            builder.append(" AND ");
            appendFrameBound(builder, style, frameDefinition.end());
        }
        builder.append(")");
        return builder.toString();
    }

    private void appendFrameBound(StringBuilder builder, Style style, FrameBoundDefinition frameBound) {
        switch (frameBound.type()) {
            case UNBOUNDED_PRECEDING:
                builder.append("UNBOUNDED PRECEDING");
                break;
            case PRECEDING:
                builder.append(frameBound.value().toString(style));
                builder.append(" PRECEDING");
                break;
            case CURRENT_ROW:
                builder.append("CURRENT ROW");
                break;
            case FOLLOWING:
                builder.append(frameBound.value().toString(style));
                builder.append("FOLLOWING");
                break;
            case UNBOUNDED_FOLLOWING:
                builder.append("UNBOUNDED FOLLOWING");
                break;

            default:
                throw new AssertionError("Unexpected frame bound type: " + frameBound.type());
        }
    }
}

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

package io.crate.expression.symbol;

import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WindowDefinition;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static io.crate.metadata.FunctionInfo.Type.AGGREGATE;
import static io.crate.metadata.FunctionInfo.Type.WINDOW;

public class WindowFunction extends Function {

    private final WindowDefinition windowDefinition;

    public WindowFunction(StreamInput in) throws IOException {
        super(in);
        windowDefinition = new WindowDefinition(in);
    }

    public WindowFunction(FunctionInfo info,
                          List<Symbol> arguments,
                          @Nullable Symbol filter,
                          WindowDefinition windowDefinition) {
        this(info, null, arguments, filter, windowDefinition);
    }

    public WindowFunction(FunctionInfo info,
                          Signature signature,
                          List<Symbol> arguments,
                          @Nullable Symbol filter,
                          WindowDefinition windowDefinition) {
        super(info, signature, arguments, filter);
        assert info.type() == WINDOW || info.type() == AGGREGATE :
            "only window and aggregate functions are allowed to be modelled over a window";
        this.windowDefinition = windowDefinition;
    }

    public WindowDefinition windowDefinition() {
        return windowDefinition;
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
        return windowDefinition.equals(that.windowDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowDefinition);
    }

    @Override
    public String toString(Style style) {
        var builder = new StringBuilder(super.toString(style));
        builder.append(" OVER (");

        var partitions = windowDefinition.partitions();
        if (!partitions.isEmpty()) {
            builder.append("PARTITION BY ");
            builder.append(Lists2.joinOn(", ", partitions, x -> x.toString(style)));
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

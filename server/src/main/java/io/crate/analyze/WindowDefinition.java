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

package io.crate.analyze;


import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;

/**
 * Representation of a window used to describe the window function calls target.
 */
public class WindowDefinition implements Writeable {

    public static final WindowFrameDefinition RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW = new WindowFrameDefinition(
        RANGE,
        new FrameBoundDefinition(UNBOUNDED_PRECEDING, Literal.NULL),
        new FrameBoundDefinition(CURRENT_ROW, Literal.NULL)
    );

    private final List<Symbol> partitions;
    @Nullable
    private final OrderBy orderBy;
    private final WindowFrameDefinition windowFrameDefinition;

    public WindowDefinition(StreamInput in) throws IOException {
        partitions = Symbols.fromStream(in);
        orderBy = in.readOptionalWriteable(OrderBy::new);
        windowFrameDefinition = new WindowFrameDefinition(in);
    }

    public WindowDefinition(List<Symbol> partitions,
                            @Nullable OrderBy orderBy,
                            @Nullable WindowFrameDefinition windowFrameDefinition) {
        this.partitions = partitions;
        this.orderBy = orderBy;
        this.windowFrameDefinition = windowFrameDefinition == null ? RANGE_UNBOUNDED_PRECEDING_CURRENT_ROW : windowFrameDefinition;
    }

    public List<Symbol> partitions() {
        return partitions;
    }

    public WindowDefinition map(Function<? super Symbol, ? extends Symbol> mapper) {
        return new WindowDefinition(
            Lists.map(partitions, mapper),
            orderBy != null ? orderBy.map(mapper) : null,
            windowFrameDefinition.map(mapper)
        );
    }

    @Nullable
    public OrderBy orderBy() {
        return orderBy;
    }

    public WindowFrameDefinition windowFrameDefinition() {
        return windowFrameDefinition;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(partitions.size());
        for (Symbol partition : partitions) {
            Symbol.toStream(partition, out);
        }
        out.writeOptionalWriteable(orderBy);
        windowFrameDefinition.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowDefinition that = (WindowDefinition) o;
        return Objects.equals(partitions, that.partitions) &&
               Objects.equals(orderBy, that.orderBy) &&
               Objects.equals(windowFrameDefinition, that.windowFrameDefinition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitions, orderBy, windowFrameDefinition);
    }

    @Override
    public String toString() {
        return "WindowDefinition{" +
               "partitions=" + partitions +
               ", orderBy=" + orderBy +
               ", windowFrame=" + windowFrameDefinition +
               '}';
    }
}

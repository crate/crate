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

package io.crate.analyze;


import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.crate.sql.tree.WindowFrame.Type.RANGE;

/**
 * Representation of a window used to describe the window function calls target.
 */
public class WindowDefinition implements Writeable {

    public static final WindowFrameDefinition DEFAULT_WINDOW_FRAME = new WindowFrameDefinition(
        RANGE,
        new FrameBoundDefinition(UNBOUNDED_PRECEDING),
        new FrameBoundDefinition(CURRENT_ROW)
    );

    private final List<Symbol> partitions;
    @Nullable
    private final OrderBy orderBy;
    private final WindowFrameDefinition windowFrameDefinition;

    public WindowDefinition(StreamInput in) throws IOException {
        partitions = Symbols.listFromStream(in);
        orderBy = in.readOptionalWriteable(OrderBy::new);
        windowFrameDefinition = in.readOptionalWriteable(WindowFrameDefinition::new);
    }

    public WindowDefinition(List<Symbol> partitions,
                            @Nullable OrderBy orderBy,
                            @Nullable WindowFrameDefinition windowFrameDefinition) {
        this.partitions = partitions;
        this.orderBy = orderBy;
        if (windowFrameDefinition != null) {
            this.windowFrameDefinition = windowFrameDefinition;
        } else {
            this.windowFrameDefinition = DEFAULT_WINDOW_FRAME;
        }
    }

    public List<Symbol> partitions() {
        return partitions;
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
            Symbols.toStream(partition, out);
        }
        out.writeOptionalWriteable(orderBy);
        out.writeOptionalWriteable(windowFrameDefinition);
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

    public String representation() {
        StringBuilder sb = new StringBuilder("Window{");
        if (partitions.size() > 0) {
            sb.append("PARTITION BY ");
            for (ExplainLeaf partition : partitions) {
                sb.append(partition.representation());
            }
        }
        if (orderBy != null) {
            sb.append(" ");
            sb.append(orderBy.toString());
        }

        if (windowFrameDefinition != null) {
            sb.append(" ");
            sb.append(windowFrameDefinition.toString());
        }

        sb.append("}");
        return sb.toString();
    }

}

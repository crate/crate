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

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.sql.tree.FrameBound;
import io.crate.sql.tree.WindowFrame.Mode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public class WindowFrameDefinition implements Writeable {

    private final Mode mode;
    private final FrameBoundDefinition start;
    private final FrameBoundDefinition end;

    public WindowFrameDefinition(StreamInput in) throws IOException {
        mode = in.readEnum(Mode.class);
        start = new FrameBoundDefinition(in);
        end = in.readOptionalWriteable(FrameBoundDefinition::new);
    }

    public WindowFrameDefinition(Mode mode, FrameBoundDefinition start, @Nullable FrameBoundDefinition end) {
        this.mode = mode;
        this.start = start;
        if (end != null) {
            this.end = end;
        } else {
            this.end = new FrameBoundDefinition(FrameBound.Type.CURRENT_ROW, Literal.NULL);
        }
    }

    public Mode mode() {
        return mode;
    }

    public FrameBoundDefinition start() {
        return start;
    }

    public FrameBoundDefinition end() {
        return end;
    }

    public WindowFrameDefinition map(Function<? super Symbol, ? extends Symbol> mapper) {
        FrameBoundDefinition newStart = start.map(mapper);
        FrameBoundDefinition newEnd = end.map(mapper);
        if (newStart == start && newEnd == end) {
            return this;
        } else {
            return new WindowFrameDefinition(mode, newStart, newEnd);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(mode);
        start.writeTo(out);
        out.writeOptionalWriteable(end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowFrameDefinition that = (WindowFrameDefinition) o;
        return mode == that.mode &&
               Objects.equals(start, that.start) &&
               Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, start, end);
    }

    @Override
    public String toString() {
        return "WindowFrame{" +
               "type=" + mode +
               ", start=" + start +
               ", end=" + end +
               '}';
    }
}

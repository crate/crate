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
import java.util.List;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.collections.Lists;
import io.crate.common.collections.MapBuilder;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.types.DataType;

public class LimitAndOffsetProjection extends Projection {

    private final int limit;
    private final int offset;
    private final List<Symbol> outputs;

    public LimitAndOffsetProjection(int limit, int offset, List<DataType<?>> outputTypes) {
        this.limit = limit;
        this.offset = offset;
        this.outputs = InputColumn.mapToInputColumns(outputTypes);
    }

    public LimitAndOffsetProjection(StreamInput in) throws IOException {
        offset = in.readVInt();
        limit = in.readVInt();
        outputs = Symbols.listFromStream(in);
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.LIMITANDOFFSET;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitLimitAndOffsetProjection(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(offset);
        out.writeVInt(limit);
        Symbols.toStream(outputs, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LimitAndOffsetProjection that = (LimitAndOffsetProjection) o;

        if (limit != that.limit) return false;
        if (offset != that.offset) return false;
        if (!outputs.equals(that.outputs)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = limit;
        result = 31 * result + offset;
        result = 31 * result + outputs.hashCode();
        return result;
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return MapBuilder.<String, Object>newMapBuilder()
            .put("type", "LimitAndOffset")
            .put("limit", limit)
            .put("offset", offset)
            .put("outputs", Lists.joinOn(", ", outputs, Symbol::toString))
            .map();
    }

    @Override
    public String toString() {
        return "LimitAndOffsetProjection{" +
               "outputs=" + outputs +
               ", limit=" + limit +
               ", offset=" + offset +
               '}';
    }

}

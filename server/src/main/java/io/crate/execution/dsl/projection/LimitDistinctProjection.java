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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.RowGranularity;

public class LimitDistinctProjection extends Projection {

    private final int limit;
    private final List<Symbol> outputs;
    private final RowGranularity granularity;

    public LimitDistinctProjection(int limit, List<Symbol> outputs, RowGranularity granularity) {
        this.limit = limit;
        this.outputs = outputs;
        this.granularity = granularity;
    }

    public LimitDistinctProjection(StreamInput in) throws IOException {
        this.limit = in.readVInt();
        this.outputs = Symbols.fromStream(in);
        this.granularity = RowGranularity.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(limit);
        Symbols.toStream(outputs, out);
        RowGranularity.toStream(granularity, out);
    }

    @Override
    public RowGranularity requiredGranularity() {
        return granularity;
    }

    public int limit() {
        return limit;
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.LIMIT_DISTINCT;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitLimitDistinct(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LimitDistinctProjection that = (LimitDistinctProjection) o;
        if (limit != that.limit) {
            return false;
        }
        if (!outputs.equals(that.outputs)) {
            return false;
        }
        return granularity == that.granularity;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + limit;
        result = 31 * result + outputs.hashCode();
        result = 31 * result + granularity.hashCode();
        return result;
    }
}

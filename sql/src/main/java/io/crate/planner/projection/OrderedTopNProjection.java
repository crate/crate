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

package io.crate.planner.projection;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitors;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.planner.ExplainLeaf;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class OrderedTopNProjection extends Projection {

    private final int limit;
    private final int offset;
    private final List<Symbol> outputs;
    private final List<Symbol> orderBy;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;

    public OrderedTopNProjection(int limit,
                                 int offset,
                                 List<Symbol> outputs,
                                 List<Symbol> orderBy,
                                 boolean[] reverseFlags,
                                 Boolean[] nullsFirst) {
        assert outputs.stream().noneMatch(s -> SymbolVisitors.any(Symbols.IS_COLUMN, s))
            : "OrderedTopNProjection outputs cannot contain Field or Reference symbols: " + outputs;
        assert orderBy.stream().noneMatch(s -> SymbolVisitors.any(Symbols.IS_COLUMN, s))
            : "OrderedTopNProjection orderBy cannot contain Field or Reference symbols: " + orderBy;
        assert orderBy.size() == reverseFlags.length : "reverse flags length does not match orderBy items count";
        assert orderBy.size() == nullsFirst.length : "nullsFirst length does not match orderBy items count";

        this.limit = limit;
        this.offset = offset;
        this.outputs = outputs;
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
    }

    public OrderedTopNProjection(StreamInput in) throws IOException {
        limit = in.readVInt();
        offset = in.readVInt();
        outputs = Symbols.listFromStream(in);
        int numOrderBy = in.readVInt();
        if (numOrderBy == 0) {
            orderBy = Collections.emptyList();
            reverseFlags = new boolean[0];
            nullsFirst = new Boolean[0];
        } else {
            orderBy = new ArrayList<>(numOrderBy);
            reverseFlags = new boolean[numOrderBy];
            nullsFirst = new Boolean[numOrderBy];
            for (int i = 0; i < numOrderBy; i++) {
                orderBy.add(Symbols.fromStream(in));
                reverseFlags[i] = in.readBoolean();
                nullsFirst[i] = in.readOptionalBoolean();
            }
        }
    }

    public List<Symbol> orderBy() {
        return orderBy;
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    @Override
    public void replaceSymbols(Function<Symbol, Symbol> replaceFunction) {
        Lists2.replaceItems(outputs, replaceFunction);
        Lists2.replaceItems(orderBy, replaceFunction);
    }

    @Override
    public ProjectionType projectionType() {
        return ProjectionType.TOPN_ORDERED;
    }

    @Override
    public <C, R> R accept(ProjectionVisitor<C, R> visitor, C context) {
        return visitor.visitOrderedTopN(this, context);
    }

    @Override
    public List<? extends Symbol> outputs() {
        return outputs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(limit);
        out.writeVInt(offset);
        Symbols.toStream(outputs, out);
        out.writeVInt(orderBy.size());
        for (int i = 0; i < orderBy.size(); i++) {
            Symbols.toStream(orderBy.get(i), out);
            out.writeBoolean(reverseFlags[i]);
            out.writeOptionalBoolean(nullsFirst[i]);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OrderedTopNProjection that = (OrderedTopNProjection) o;

        if (limit != that.limit) return false;
        if (offset != that.offset) return false;
        if (!outputs.equals(that.outputs)) return false;
        if (!orderBy.equals(that.orderBy)) return false;
        if (!Arrays.equals(reverseFlags, that.reverseFlags)) return false;
        return Arrays.equals(nullsFirst, that.nullsFirst);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + limit;
        result = 31 * result + offset;
        result = 31 * result + outputs.hashCode();
        result = 31 * result + orderBy.hashCode();
        result = 31 * result + Arrays.hashCode(reverseFlags);
        result = 31 * result + Arrays.hashCode(nullsFirst);
        return result;
    }

    @Override
    public Map<String, Object> mapRepresentation() {
        return ImmutableMap.of(
            "type", "OrderByTopN",
            "limit", limit,
            "offset", offset,
            "outputs", ExplainLeaf.printList(outputs),
            "orderBy", OrderBy.explainRepresentation(
                new StringBuilder("["), orderBy, reverseFlags, nullsFirst).append("]").toString()
        );
    }
}

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

package io.crate.planner;

import io.crate.analyze.OrderBy;
import io.crate.expression.symbol.Symbol;
import io.crate.planner.consumer.OrderByPositionVisitor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import org.jspecify.annotations.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PositionalOrderBy {

    private final int[] indices;
    private final boolean[] reverseFlags;
    private final boolean[] nullsFirst;

    public PositionalOrderBy(int[] indices, boolean[] reverseFlags, boolean[] nullsFirst) {
        assert indices.length == reverseFlags.length && reverseFlags.length == nullsFirst.length
            : "all parameters to OrderByPositions must have the same length";
        // PositionalOrderBy should be null if there is no order by
        assert indices.length > 0 : "parameters must have length > 0";

        this.indices = indices;
        this.reverseFlags = reverseFlags;
        this.nullsFirst = nullsFirst;
    }

    public int[] indices() {
        return indices;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public boolean[] nullsFirst() {
        return nullsFirst;
    }

    @Override
    public String toString() {
        return "OrderByPositions{" +
               "indices=" + Arrays.toString(indices) +
               ", reverseFlags=" + Arrays.toString(reverseFlags) +
               ", nullsFirst=" + Arrays.toString(nullsFirst) +
               '}';
    }

    /**
     * Returns a new PositionalOrderBy where the indices are changed so that it uses the newOutputs as base.
     * <br />
     * Example:
     * <pre>
     *     select b, a from (select a, b .. order by b desc, a asc nulls first)
     *
     *                                            b        a
     *     oldOutputs:  [a, b]  -> orderByIndices 1 DESC , 0 ASC NULLS FIRST
     *
     *                                            b        a
     *     newOutputs:  [b, a]  -> orderByIndices 0 DESC , 1 ASC NULLS FIRST
     * </pre>
     *
     * If the newOutputs don't contain a symbol that was used in the oldOutputs `null` is returned.
     */
    @Nullable
    public PositionalOrderBy tryMapToNewOutputs(List<Symbol> oldOutputs, List<Symbol> newOutputs) {
        int[] newIndices = new int[indices.length];

        for (int i = 0; i < indices.length; i++) {
            int idxInOldOutputs = indices[i];
            Symbol orderByExpr = oldOutputs.get(idxInOldOutputs);
            int idxInNewOutputs = newOutputs.indexOf(orderByExpr);
            if (idxInNewOutputs < 0) {
                return null;
            } else {
                newIndices[i] = idxInNewOutputs;
            }
        }
        return new PositionalOrderBy(
            newIndices,
            Arrays.copyOf(reverseFlags, reverseFlags.length),
            Arrays.copyOf(nullsFirst, nullsFirst.length)
        );
    }

    @Nullable
    public static PositionalOrderBy fromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        int[] indices = new int[size];
        boolean[] reverseFlags = new boolean[size];
        boolean[] nullsFirst = new boolean[size];
        for (int i = 0; i < size; i++) {
            indices[i] = in.readVInt();
            reverseFlags[i] = in.readBoolean();
            nullsFirst[i] = in.readBoolean();
        }
        return new PositionalOrderBy(indices, reverseFlags, nullsFirst);
    }

    public static void toStream(@Nullable PositionalOrderBy orderBy, StreamOutput out) throws IOException {
        if (orderBy == null) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(orderBy.indices.length);
        for (int i = 0; i < orderBy.indices.length; i++) {
            out.writeVInt(orderBy.indices[i]);
            out.writeBoolean(orderBy.reverseFlags[i]);
            out.writeBoolean(orderBy.nullsFirst[i]);
        }
    }

    /**
     * Create OrderByPositions from a OrderBy definition.
     *
     * @param orderByInputSymbols symbols which describe the output which should be sorted.
     *                            {@link OrderBy#orderBySymbols()} must point to those symbols.
     *                            This is used to create the correct indices mapping.
     */
    @Nullable
    public static PositionalOrderBy of(@Nullable OrderBy orderBy, List<? extends Symbol> orderByInputSymbols) {
        if (orderBy == null) {
            return null;
        }
        int[] indices = OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), orderByInputSymbols);
        if (indices.length == 0) {
            return null;
        }
        return new PositionalOrderBy(indices, orderBy.reverseFlags(), orderBy.nullsFirst());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PositionalOrderBy that = (PositionalOrderBy) o;
        return Arrays.equals(indices, that.indices) &&
               Arrays.equals(reverseFlags, that.reverseFlags) &&
               Arrays.equals(nullsFirst, that.nullsFirst);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(reverseFlags);
        result = 31 * result + Arrays.hashCode(nullsFirst);
        return result;
    }
}

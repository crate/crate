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

package io.crate.planner;

import io.crate.analyze.OrderBy;
import io.crate.analyze.symbol.Symbol;
import io.crate.planner.consumer.OrderByPositionVisitor;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PositionalOrderBy {

    private final int[] indices;
    private final boolean[] reverseFlags;
    private final Boolean[] nullsFirst;

    private PositionalOrderBy(int[] indices, boolean[] reverseFlags, Boolean[] nullsFirst) {
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

    public Boolean[] nullsFirst() {
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

    @Nullable
    public static PositionalOrderBy fromStream(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        int[] indices = new int[size];
        boolean[] reverseFlags = new boolean[size];
        Boolean[] nullsFirst = new Boolean[size];
        for (int i = 0; i < size; i++) {
            indices[i] = in.readVInt();
            reverseFlags[i] = in.readBoolean();
            nullsFirst[i] = in.readOptionalBoolean();
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
            out.writeOptionalBoolean(orderBy.nullsFirst[i]);
        }
    }

    /**
     * Create OrderByPositions from a OrderBy definition.
     *
     * The indices will simply be incremental with as many items as there are orderBySymbols in {@code orderBy}.
     */
    @Nullable
    public static PositionalOrderBy of(@Nullable OrderBy orderBy) {
        if (orderBy == null) {
            return null;
        }
        int[] indices = new int[orderBy.orderBySymbols().size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        return new PositionalOrderBy(indices, orderBy.reverseFlags(), orderBy.nullsFirst());
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
        return new PositionalOrderBy(indices, orderBy.reverseFlags(), orderBy.nullsFirst());
    }
}

/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.planner.node.dql;


import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ReferenceInfo;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.List;

public class ESSearchNode extends ESDQLPlanNode {

    private final List<Reference> orderBy;
    private final int limit;
    private final int offset;
    private final boolean[] reverseFlags;
    private final WhereClause whereClause;
    private final String[] indices;

    private final List<ReferenceInfo> partitionBy;
    private final Boolean[] nullsFirst;

    /**
     *
     * @param partitionBy list of columns
     *                    the queried is partitioned by
     */
    public ESSearchNode(String[] indices,
                        List<Symbol> outputs,
                        @Nullable List<Reference> orderBy,
                        @Nullable boolean[] reverseFlags,
                        @Nullable Boolean[] nullsFirst,
                        @Nullable Integer limit,
                        @Nullable Integer offset,
                        WhereClause whereClause,
                        @Nullable List<ReferenceInfo> partitionBy
                        ) {
        assert indices != null && indices.length > 0;
        assert outputs != null;
        assert whereClause != null;
        this.indices = indices;
        this.orderBy = Objects.firstNonNull(orderBy, ImmutableList.<Reference>of());
        this.reverseFlags = Objects.firstNonNull(reverseFlags, new boolean[0]);
        this.nullsFirst = Objects.firstNonNull(nullsFirst, new Boolean[0]);
        Preconditions.checkArgument(this.orderBy.size() == this.reverseFlags.length,
                "orderBy size doesn't match with reverseFlag length");

        this.whereClause = whereClause;
        this.outputs = outputs;

        // TODO: move constant to some other location?
        this.limit = Objects.firstNonNull(limit, Constants.DEFAULT_SELECT_LIMIT);
        this.offset = Objects.firstNonNull(offset, 0);

        this.partitionBy = Objects.firstNonNull(partitionBy, ImmutableList.<ReferenceInfo>of());
    }

    public String[] indices(){
        return indices;
    }

    @Override
    public List<? extends Reference> outputs() {
        return (List<? extends Reference>) super.outputs();
    }

    public int limit() {
        return limit;
    }

    public int offset() {
        return offset;
    }

    public Boolean[] nullsFirst() {
        return nullsFirst;
    }

    public boolean[] reverseFlags() {
        return reverseFlags;
    }

    public List<Reference> orderBy() {
        return orderBy;
    }

    public List<ReferenceInfo> partitionBy() {
        return partitionBy;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESSearchNode(this, context);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("offset", offset())
                .add("limit", limit())
                .add("orderBy", orderBy())
                .add("reverseFlags", Arrays.toString(reverseFlags()))
                .add("whereClause", whereClause())
                .add("partitionBy", partitionBy)
                .toString();
    }
}


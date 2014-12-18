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


import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.planner.node.PlanVisitor;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import org.elasticsearch.common.Nullable;

import java.util.Arrays;
import java.util.List;

public class QueryThenFetchNode extends ESDQLPlanNode {

    private final List<Symbol> orderBy;
    private final int limit;
    private final int offset;
    private final boolean[] reverseFlags;
    private final WhereClause whereClause;

    private final List<ReferenceInfo> partitionBy;
    private final Boolean[] nullsFirst;
    private final Routing routing;

    private static final Boolean[] EMPTY_OBJ_BOOLEAN_ARR = new Boolean[0];
    private static final boolean[] EMPTY_VALUE_BOOLEAN_ARR = new boolean[0];

    /**
     *
     * @param partitionBy list of columns
     *                    the queried is partitioned by
     */
    public QueryThenFetchNode(Routing routing,
                              List<Symbol> outputs,
                              @Nullable List<Symbol> orderBy,
                              @Nullable boolean[] reverseFlags,
                              @Nullable Boolean[] nullsFirst,
                              @Nullable Integer limit,
                              @Nullable Integer offset,
                              WhereClause whereClause,
                              @Nullable List<ReferenceInfo> partitionBy
    ) {
        this.routing = routing;
        assert routing != null;
        assert outputs != null;
        assert whereClause != null;
        this.orderBy = MoreObjects.firstNonNull(orderBy, ImmutableList.<Symbol>of());
        this.reverseFlags = MoreObjects.firstNonNull(reverseFlags, EMPTY_VALUE_BOOLEAN_ARR);
        this.nullsFirst = MoreObjects.firstNonNull(nullsFirst, EMPTY_OBJ_BOOLEAN_ARR);
        Preconditions.checkArgument(this.orderBy.size() == this.reverseFlags.length,
                "orderBy size doesn't match with reverseFlag length");

        this.whereClause = whereClause;
        this.outputs = outputs;

        this.limit = MoreObjects.firstNonNull(limit, Constants.DEFAULT_SELECT_LIMIT);
        this.offset = MoreObjects.firstNonNull(offset, 0);

        this.partitionBy = MoreObjects.firstNonNull(partitionBy, ImmutableList.<ReferenceInfo>of());
        outputTypes(Symbols.extractTypes(outputs));
    }

    public Routing routing() {
        return routing;
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

    public List<Symbol> orderBy() {
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
        return visitor.visitQueryThenFetchNode(this, context);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("offset", offset())
                .add("limit", limit())
                .add("orderBy", orderBy())
                .add("reverseFlags", Arrays.toString(reverseFlags()))
                .add("whereClause", whereClause())
                .add("partitionBy", partitionBy)
                .toString();
    }
}

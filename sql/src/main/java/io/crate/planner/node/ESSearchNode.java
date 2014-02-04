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

package io.crate.planner.node;


import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.cratedb.Constants;
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Set;

public class ESSearchNode extends PlanNode {

    private final List<Reference> orderBy;
    private final int limit;
    private final int offset;
    private final boolean[] reverseFlags;
    private final Optional<Function> whereClause;
    private List<Symbol> outputs;

    public ESSearchNode(List<Symbol> outputs,
                        @Nullable List<Reference> orderBy,
                        @Nullable boolean[] reverseFlags,
                        @Nullable Integer limit,
                        @Nullable Integer offset,
                        @Nullable Function whereClause) {
        Preconditions.checkNotNull(outputs);
        this.orderBy = Objects.firstNonNull(orderBy, ImmutableList.<Reference>of());
        this.reverseFlags = Objects.firstNonNull(reverseFlags, new boolean[0]);
        Preconditions.checkArgument(this.orderBy.size() == this.reverseFlags.length,
                "orderBy size doesn't match with reverseFlag length");

        this.whereClause = Optional.fromNullable(whereClause);
        this.outputs = outputs;

        // TODO: move constant to some other location?
        this.limit = Objects.firstNonNull(limit, Constants.DEFAULT_SELECT_LIMIT);
        this.offset = Objects.firstNonNull(offset, 0);
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    public void outputs(List<Symbol> outputs) {
        this.outputs = outputs;
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

    public List<Reference> orderBy() {
        return orderBy;
    }

    public Optional<Function> whereClause() {
        return whereClause;
    }

    @Override
    public Set<String> executionNodes() {
        // always runs on mapper since it uses its own routing internally
        return ImmutableSet.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.visitESSearchNode(this, context);
    }
}


/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import javax.annotation.Nullable;
import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

public class QuerySpec {

    private List<Symbol> groupBy;
    private OrderBy orderBy;
    private Symbol having;
    private List<Symbol> outputs;
    private WhereClause where;
    private Integer limit;
    private Integer offset = 0;
    private boolean hasAggregates = false;

    @Nullable
    public List<Symbol> groupBy() {
        return groupBy;
    }

    public QuerySpec groupBy(@Nullable List<Symbol> groupBy) {
        this.groupBy = groupBy != null && groupBy.size() > 0 ? groupBy : null;
        return this;
    }

    @Nullable
    public WhereClause where() {
        return where;
    }

    public QuerySpec where(@Nullable WhereClause where) {
        this.where = where;
        return this;
    }

    @Nullable
    public Integer limit() {
        return limit;
    }

    public QuerySpec limit(@Nullable Integer limit) {
        this.limit = limit;
        return this;
    }

    public Integer offset() {
        return offset;
    }

    public QuerySpec offset(@Nullable Integer offset) {
        this.offset = offset != null ? offset : 0;
        return this;
    }

    @Nullable
    public Symbol having() {
        return having;
    }

    public QuerySpec having(@Nullable Symbol having) {
        this.having = having;
        return this;
    }

    @Nullable
    public OrderBy orderBy() {
        return orderBy;
    }

    public QuerySpec orderBy(@Nullable OrderBy orderBy) {
        this.orderBy = orderBy;
        return this;
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    public QuerySpec outputs(List<Symbol> outputs) {
        this.outputs = outputs;
        return this;
    }

    public boolean hasAggregates() {
        return hasAggregates;
    }

    public QuerySpec hasAggregates(boolean hasAggregates) {
        this.hasAggregates = hasAggregates;
        return this;
    }

    public boolean isLimited() {
        return limit != null || offset > 0;
    }

    public void normalize(EvaluatingNormalizer normalizer) {
        normalizer.normalizeInplace(groupBy);
        orderBy.normalize(normalizer);
        normalizer.normalizeInplace(outputs);
        where = where.normalize(normalizer);
    }

    public boolean hasNoResult() {
        if (having != null && having.symbolType() == SymbolType.LITERAL) {
            Literal havingLiteral = (Literal) having;
            if (havingLiteral.value() == false) {
                return true;
            }
        }
        if (hasAggregates() && groupBy() == null) {
            return firstNonNull(limit(), 1) < 1 || offset() > 0;
        }
        return where.noMatch() || limit != null && limit == 0;
    }

}
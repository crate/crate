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

import io.crate.common.collections.Lists2;
import io.crate.expression.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

public final class QuerySpec {

    private final List<Symbol> outputs;
    private final WhereClause where;
    private final List<Symbol> groupBy;

    @Nullable
    private final HavingClause having;

    @Nullable
    private final OrderBy orderBy;

    @Nullable
    private final Symbol limit;

    @Nullable
    private final Symbol offset;

    private final boolean hasAggregates;

    public QuerySpec(List<Symbol> outputs,
                     WhereClause where,
                     List<Symbol> groupBy,
                     @Nullable HavingClause having,
                     @Nullable OrderBy orderBy,
                     @Nullable Symbol limit,
                     @Nullable Symbol offset,
                     boolean hasAggregates) {
        this.outputs = outputs;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
        this.hasAggregates = hasAggregates;
    }

    public List<Symbol> groupBy() {
        return groupBy;
    }

    public WhereClause where() {
        return where;
    }

    @Nullable
    public Symbol limit() {
        return limit;
    }

    @Nullable
    public Symbol offset() {
        return offset;
    }

    @Nullable
    public HavingClause having() {
        return having;
    }

    @Nullable
    public OrderBy orderBy() {
        return orderBy;
    }

    public List<Symbol> outputs() {
        return outputs;
    }

    public boolean hasAggregates() {
        return hasAggregates;
    }

    public QuerySpec map(Function<? super Symbol, ? extends Symbol> mapper) {
        return new QuerySpec(
            Lists2.map(outputs, mapper),
            where.map(mapper),
            Lists2.map(groupBy, mapper),
            having == null ? null : having.map(mapper),
            orderBy == null ? null : orderBy.map(mapper),
            limit == null ? null : mapper.apply(limit),
            offset == null ? null : mapper.apply(offset),
            hasAggregates
        );
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH,
            "QS{ SELECT %s WHERE %s GROUP BY %s HAVING %s ORDER BY %s LIMIT %s OFFSET %s}",
            outputs, where, groupBy, having, orderBy, limit, offset);
    }
}

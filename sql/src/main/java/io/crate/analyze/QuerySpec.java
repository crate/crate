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

import io.crate.collections.Lists2;
import io.crate.expression.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;

public class QuerySpec {

    private List<Symbol> outputs = Collections.emptyList();
    private WhereClause where = WhereClause.MATCH_ALL;
    private List<Symbol> groupBy = Collections.emptyList();
    private HavingClause having = null;
    private OrderBy orderBy = null;

    @Nullable
    private Symbol limit = null;

    @Nullable
    private Symbol offset = null;

    private boolean hasAggregates = false;

    public QuerySpec groupBy(@Nullable List<Symbol> groupBy) {
        this.groupBy = firstNonNull(groupBy, Collections.emptyList());
        return this;
    }

    public List<Symbol> groupBy() {
        return groupBy;
    }

    public WhereClause where() {
        return where;
    }

    public QuerySpec where(@Nullable WhereClause where) {
        if (where == null) {
            this.where = WhereClause.MATCH_ALL;
        } else {
            this.where = where;
        }
        return this;
    }

    @Nullable
    public Symbol limit() {
        return limit;
    }

    public QuerySpec limit(@Nullable Symbol limit) {
        this.limit = limit;
        return this;
    }

    @Nullable
    public Symbol offset() {
        return offset;
    }

    public QuerySpec offset(@Nullable Symbol offset) {
        this.offset = offset;
        return this;
    }

    @Nullable
    public HavingClause having() {
        return having;
    }

    public QuerySpec having(@Nullable HavingClause having) {
        if (having == null || !having.hasQuery() && !having.noMatch()) {
            this.having = null;
        } else {
            this.having = having;
        }
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

    public QuerySpec copyAndReplace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        QuerySpec newSpec = new QuerySpec()
            .limit(limit)
            .offset(offset)
            .hasAggregates(hasAggregates)
            .outputs(Lists2.copyAndReplace(outputs, replaceFunction));
        newSpec.where(where.copyAndReplace(replaceFunction));
        if (orderBy != null) {
            newSpec.orderBy(orderBy.copyAndReplace(replaceFunction));
        }
        if (having != null) {
            if (having.hasQuery()) {
                newSpec.having(new HavingClause(replaceFunction.apply(having.query)));
            }
        }
        if (!groupBy.isEmpty()) {
            newSpec.groupBy(Lists2.copyAndReplace(groupBy, replaceFunction));
        }
        return newSpec;
    }

    public void replace(Function<? super Symbol, ? extends Symbol> replaceFunction) {
        Lists2.replaceItems(outputs, replaceFunction);
        where.replace(replaceFunction);
        if (orderBy != null) {
            orderBy.replace(replaceFunction);
        }
        Lists2.replaceItems(groupBy, replaceFunction);
        if (limit != null) {
            limit = replaceFunction.apply(limit);
        }
        if (offset != null) {
            offset = replaceFunction.apply(offset);
        }
        if (having != null) {
            having.replace(replaceFunction);
        }
    }

    /**
     * Visit all symbols present in this query spec.
     * <p>
     * (non-recursive, so function symbols won't be walked into)
     * </p>
     */
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (Symbol output : outputs) {
            consumer.accept(output);
        }
        if (where.hasQuery()) {
            consumer.accept(where.query());
        }
        for (Symbol groupBySymbol : groupBy) {
            consumer.accept(groupBySymbol);
        }
        if (having != null && having.hasQuery()) {
            consumer.accept(having.query());
        }
        if (orderBy != null) {
            for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
                consumer.accept(orderBySymbol);
            }
        }
        if (limit != null) {
            consumer.accept(limit);
        }
        if (offset != null) {
            consumer.accept(offset);
        }
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH,
            "QS{ SELECT %s WHERE %s GROUP BY %s HAVING %s ORDER BY %s LIMIT %s OFFSET %s}",
            outputs, where, groupBy, having, orderBy, limit, offset);
    }
}

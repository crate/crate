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

import io.crate.expression.symbol.Symbol;
import io.crate.collections.Lists2;
import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
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

    /**
     * Tries to cast the outputs to the given types. Types might contain Null values, which indicates to skip that
     * position. The iterator needs to return exactly the same number of types as there are outputs.
     *
     * @param types an iterable providing the types
     * @return -1 if all casts where successfully applied or the position of the failed cast
     */
    public int castOutputs(Iterator<DataType> types) {
        int i = 0;
        ListIterator<Symbol> outputsIt = outputs.listIterator();
        while (types.hasNext() && outputsIt.hasNext()) {
            DataType targetType = types.next();
            assert targetType != null : "targetType must not be null";

            Symbol output = outputsIt.next();
            DataType sourceType = output.valueType();
            if (!sourceType.equals(targetType)) {
                if (sourceType.isConvertableTo(targetType)) {
                    Symbol castFunction = CastFunctionResolver.generateCastFunction(output, targetType, false);
                    Collections.replaceAll(groupBy, output, castFunction);
                    if (orderBy != null) {
                        Collections.replaceAll(orderBy.orderBySymbols(), output, castFunction);
                    }
                    outputsIt.set(castFunction);
                } else if (!targetType.equals(DataTypes.UNDEFINED)) {
                    return i;
                }
            }
            i++;
        }
        assert i == outputs.size() : "i must be equal to outputs.size()";
        return -1;
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

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH,
            "QS{ SELECT %s WHERE %s GROUP BY %s HAVING %s ORDER BY %s LIMIT %s OFFSET %s}",
            outputs, where, groupBy, having, orderBy, limit, offset);
    }
}

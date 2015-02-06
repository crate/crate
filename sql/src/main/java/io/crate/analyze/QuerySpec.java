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

import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QuerySpec {

    private List<Symbol> groupBy;
    private OrderBy orderBy;
    private HavingClause having;
    private List<Symbol> outputs;
    private WhereClause where;
    private Integer limit;
    private int offset = 0;
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

    public int offset() {
        return offset;
    }

    public QuerySpec offset(@Nullable Integer offset) {
        this.offset = offset != null ? offset : 0;
        return this;
    }

    @Nullable
    public HavingClause having() {
        return having;
    }

    public QuerySpec having(@Nullable HavingClause having) {
        if (having == null || !having.hasQuery() && !having.noMatch()){
            this.having = null;
        }
        this.having = having;
        return this;
    }

    @Nullable
    public OrderBy orderBy() {
        return orderBy;
    }

    public QuerySpec orderBy(@Nullable OrderBy orderBy) {
        if (orderBy == null || !orderBy.isSorted()) {
            this.orderBy = null;
        } else {
            this.orderBy = orderBy;
        }
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
        if (groupBy != null) {
            normalizer.normalizeInplace(groupBy);
        }
        if (orderBy != null) {
            orderBy.normalize(normalizer);
        }
        if (outputs != null) {
            normalizer.normalizeInplace(outputs);
        }
        if (where != null) {
            where = where.normalize(normalizer);
        }
        if (having != null) {
            having = having.normalize(normalizer);
        }
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
        while (types.hasNext()) {
            DataType targetType = types.next();
            if (targetType != null) {
                Symbol output = outputs.get(i);
                DataType sourceType = output.valueType();
                if (!sourceType.equals(targetType)) {
                    if (sourceType.isConvertableTo(targetType)) {
                        Function castFunction = new Function(
                                CastFunctionResolver.functionInfo(sourceType, targetType),
                                Arrays.asList(output));
                        if (groupBy() != null) {
                            Collections.replaceAll(groupBy(), output, castFunction);
                        }
                        if (orderBy() != null) {
                            Collections.replaceAll(orderBy().orderBySymbols(), output, castFunction);
                        }
                        outputs.set(i, castFunction);
                    } else {
                        return i;
                    }
                }
            }
            i++;
        }
        assert i == outputs.size();
        return -1;
    }

}
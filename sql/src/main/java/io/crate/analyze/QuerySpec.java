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

import com.google.common.base.Optional;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Symbol;
import io.crate.operation.scalar.cast.CastFunctionResolver;
import io.crate.types.DataType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class QuerySpec {

    private Optional<List<Symbol>> groupBy = Optional.absent();
    private Optional<OrderBy> orderBy = Optional.absent();
    private Optional<HavingClause> having = Optional.absent();
    private List<Symbol> outputs;
    private WhereClause where = WhereClause.MATCH_ALL;
    private Optional<Integer> limit = Optional.absent();
    private int offset = 0;
    private boolean hasAggregates = false;

    public Optional<List<Symbol>> groupBy() {
        return groupBy;
    }

    public QuerySpec groupBy(@Nullable List<Symbol> groupBy) {
        assert groupBy == null || groupBy.size() > 0 : "groupBy must not be empty";
        this.groupBy = Optional.fromNullable(groupBy);
        return this;
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

    public Optional<Integer> limit() {
        return limit;
    }

    public QuerySpec limit(@Nullable Integer limit) {
        this.limit = Optional.fromNullable(limit);
        return this;
    }

    public int offset() {
        return offset;
    }

    public QuerySpec offset(@Nullable Integer offset) {
        this.offset = offset != null ? offset : 0;
        return this;
    }

    public Optional<HavingClause> having() {
        return having;
    }

    public QuerySpec having(@Nullable HavingClause having) {
        if (having == null || !having.hasQuery() && !having.noMatch()) {
            this.having = Optional.absent();
        } else {
            this.having = Optional.of(having);
        }
        return this;
    }

    public Optional<OrderBy> orderBy() {
        return orderBy;
    }

    public QuerySpec orderBy(@Nullable OrderBy orderBy) {
        this.orderBy = Optional.fromNullable(orderBy);
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
        return limit.isPresent() || offset > 0;
    }

    public void normalize(EvaluatingNormalizer normalizer) {
        if (groupBy.isPresent()) {
            normalizer.normalizeInplace(groupBy.get());
        }
        if (orderBy.isPresent()) {
            orderBy.get().normalize(normalizer);
        }
        if (outputs != null) {
            normalizer.normalizeInplace(outputs);
        }
        if (where != null && where != WhereClause.MATCH_ALL) {
            this.where(where.normalize(normalizer));
        }
        if (having.isPresent()) {
            Optional.of(having.get().normalize(normalizer));
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
                                CastFunctionResolver.functionInfo(sourceType, targetType, false),
                                Arrays.asList(output));
                        if (groupBy().isPresent()) {
                            Collections.replaceAll(groupBy().get(), output, castFunction);
                        }
                        if (orderBy().isPresent()) {
                            Collections.replaceAll(orderBy().get().orderBySymbols(), output, castFunction);
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
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

package io.crate.operation;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Row;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.InputCondition;

import javax.annotation.Nullable;

public class RowFilter implements Predicate<Row> {

    private final Input<Boolean> filterCondition;
    private final Iterable<CollectExpression<Row, ?>> filterCollectExpressions;

    public static Predicate<Row> create(InputFactory inputFactory, @Nullable Symbol filterSymbol) {
        if (filterSymbol == null) {
            return Predicates.alwaysTrue();
        }
        return new RowFilter(inputFactory, filterSymbol);
    }

    private RowFilter(InputFactory inputFactory, Symbol filterSymbol) {
        InputFactory.Context<CollectExpression<Row, ?>> ctx = inputFactory.ctxForInputColumns();
        //noinspection unchecked
        filterCondition = (Input) ctx.add(filterSymbol);
        filterCollectExpressions = ctx.expressions();
    }

    @Override
    public boolean apply(@Nullable Row row) {
        for (CollectExpression<Row, ?> collectExpression : filterCollectExpressions) {
            collectExpression.setNextRow(row);
        }
        return InputCondition.matches(filterCondition);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowFilter rowFilter = (RowFilter) o;
        return Objects.equal(filterCondition, rowFilter.filterCondition) &&
               Objects.equal(filterCollectExpressions, rowFilter.filterCollectExpressions);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(filterCondition, filterCollectExpressions);
    }
}

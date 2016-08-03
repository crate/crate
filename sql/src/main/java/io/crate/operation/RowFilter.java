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
import io.crate.core.collections.Row;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.projectors.InputCondition;

import javax.annotation.Nullable;
import java.util.Collection;

public class RowFilter implements Predicate<Row> {

    private final Input<Boolean> filterCondition;
    private final Collection<CollectExpression<Row, ?>> filterCollectExpressions;

    public static Predicate<Row> create(ImplementationSymbolVisitor symbolVisitor, @Nullable Symbol filterSymbol) {
        if (filterSymbol == null) {
            return Predicates.alwaysTrue();
        }
        return new RowFilter(symbolVisitor, filterSymbol);
    }

    private RowFilter(ImplementationSymbolVisitor symbolVisitor, Symbol filterSymbol) {
        ImplementationSymbolVisitor.Context ctx = new ImplementationSymbolVisitor.Context();
        //noinspection unchecked
        filterCondition = (Input) symbolVisitor.process(filterSymbol, ctx);
        filterCollectExpressions = ctx.collectExpressions();
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

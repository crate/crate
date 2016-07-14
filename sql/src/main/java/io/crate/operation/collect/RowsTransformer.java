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

package io.crate.operation.collect;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.projectors.InputCondition;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.types.DataTypes;

import java.util.ArrayList;
import java.util.Collections;

public final class RowsTransformer {

    public static Iterable<Row> toRowsIterable(CollectInputSymbolVisitor<?> docInputSymbolVisitor, RoutedCollectPhase collectPhase, Iterable<?> iterable) {
        WhereClause whereClause = collectPhase.whereClause();
        if (whereClause.noMatch()) {
            return Collections.emptyList();
        }

        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectPhase.toCollect());
        OrderBy orderBy = collectPhase.orderBy();
        if (orderBy != null) {
            for (Symbol symbol : orderBy.orderBySymbols()) {
                docInputSymbolVisitor.process(symbol, ctx);
            }
        }

        Input<Boolean> condition;
        if (whereClause.hasQuery()) {
            assert DataTypes.BOOLEAN.equals(whereClause.query().valueType());
            //noinspection unchecked  whereClause().query() is a symbol of type boolean so it must become Input<Boolean>
            condition = (Input<Boolean>) docInputSymbolVisitor.process(whereClause.query(), ctx);
        } else {
            condition = Literal.BOOLEAN_TRUE;
        }

        @SuppressWarnings("unchecked")
        Iterable<Row> rows = Iterables.filter(
            Iterables.transform(iterable, new ValueAndInputRow<>(ctx.topLevelInputs(), ctx.docLevelExpressions())),
            InputCondition.asPredicate(condition));

        if (orderBy == null) {
            return rows;
        }
        return sortRows(Iterables.transform(rows, Row.MATERIALIZE), collectPhase);
    }

    public static Iterable<Row> sortRows(Iterable<Object[]> rows, RoutedCollectPhase collectPhase) {
        ArrayList<Object[]> objects = Lists.newArrayList(rows);
        Ordering<Object[]> ordering = OrderingByPosition.arrayOrdering(collectPhase);
        Collections.sort(objects, ordering.reverse());
        return Iterables.transform(objects, Buckets.arrayToRowFunction());
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import org.elasticsearch.common.util.CollectionUtils;

import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.common.collections.Lists;
import io.crate.data.Buckets;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.sort.OrderingByPosition;
import io.crate.expression.InputCondition;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.TransactionContext;
import io.crate.types.DataTypes;

public final class RowsTransformer {

    public static Iterable<Row> toRowsIterable(TransactionContext txnCtx,
                                               InputFactory inputFactory,
                                               ReferenceResolver<?> referenceResolver,
                                               RoutedCollectPhase collectPhase,
                                               Iterable<?> iterable) {
        return toRowsIterable(txnCtx, inputFactory, referenceResolver, collectPhase, iterable, true);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Iterable<Row> toRowsIterable(TransactionContext txnCtx,
                                               InputFactory inputFactory,
                                               ReferenceResolver<?> referenceResolver,
                                               RoutedCollectPhase collectPhase,
                                               Iterable<?> iterable,
                                               boolean sort) {
        if (!WhereClause.canMatch(collectPhase.where())) {
            return Collections.emptyList();
        }
        InputFactory.Context ctx = inputFactory.ctxForRefs(txnCtx, referenceResolver);
        ctx.add(collectPhase.toCollect());
        OrderBy orderBy = collectPhase.orderBy();
        if (orderBy != null) {
            for (Symbol symbol : orderBy.orderBySymbols()) {
                ctx.add(symbol);
            }
        }

        ValueAndInputRow<Object> inputRow = new ValueAndInputRow<>(ctx.topLevelInputs(), ctx.expressions());
        assert DataTypes.BOOLEAN.equals(collectPhase.where().valueType()) :
            "whereClause.query() must be of type " + DataTypes.BOOLEAN;

        Predicate<Row> predicate = InputCondition.asPredicate(ctx.add(collectPhase.where()));

        if (sort == false || orderBy == null) {
            return () -> StreamSupport.stream((Spliterator<Object>) iterable.spliterator(), false)
                .map(inputRow)
                .filter(predicate)
                .iterator();
        }
        ArrayList<Object[]> items = new ArrayList<>();
        for (var item : iterable) {
            var row = inputRow.apply(item);
            if (predicate.test(row)) {
                items.add(row.materialize());
            }
        }
        items.sort(OrderingByPosition.arrayOrdering(collectPhase));
        return Lists.mapLazy(items, Buckets.arrayToSharedRow());
    }

    public static Iterable<Row> sortRows(Iterable<Object[]> rows, RoutedCollectPhase collectPhase) {
        ArrayList<Object[]> objects = CollectionUtils.iterableAsArrayList(rows);
        Comparator<Object[]> ordering = OrderingByPosition.arrayOrdering(collectPhase);
        objects.sort(ordering);
        return Lists.mapLazy(objects, Buckets.arrayToSharedRow());
    }
}

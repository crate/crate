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

package io.crate.execution.engine.sort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.crate.analyze.OrderBy;
import io.crate.common.collections.CompoundOrdering;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.types.DataType;

public final class OrderingByPosition {

    public static Comparator<Object[]> arrayOrdering(RoutedCollectPhase collectPhase) {
        OrderBy orderBy = collectPhase.orderBy();
        assert orderBy != null : "collectPhase must have an orderBy clause to generate an ordering";

        int[] positions = OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectPhase.toCollect());
        return arrayOrdering(
            Symbols.typeView(collectPhase.toCollect()),
            positions,
            orderBy.reverseFlags(),
            orderBy.nullsFirst()
        );
    }

    public static Comparator<Row> rowOrdering(List<? extends DataType<?>> rowTypes, PositionalOrderBy orderBy) {
        return rowOrdering(rowTypes, orderBy.indices(), orderBy.reverseFlags(), orderBy.nullsFirst());
    }

    public static Comparator<Row> rowOrdering(OrderBy orderBy, List<Symbol> toCollect) {
        int[] positions = OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), toCollect);
        return rowOrdering(Symbols.typeView(toCollect), positions, orderBy.reverseFlags(), orderBy.nullsFirst());
    }

    public static Comparator<Row> rowOrdering(List<? extends DataType<?>> rowTypes,
                                              int[] positions,
                                              boolean[] reverseFlags,
                                              boolean[] nullsFirst) {
        List<Comparator<Row>> comparators = new ArrayList<>(positions.length);
        for (int i = 0; i < positions.length; i++) {
            int position = positions[i];
            Comparator<Row> rowOrdering = rowOrdering(rowTypes.get(position), position, reverseFlags[i], nullsFirst[i]);
            comparators.add(rowOrdering);
        }
        return CompoundOrdering.of(comparators);
    }

    @SuppressWarnings("unchecked")
    public static <T> Comparator<Row> rowOrdering(DataType<T> type, int position, boolean reverse, boolean nullsFirst) {
        return new NullAwareComparator<>(row -> (T) row.get(position), type, reverse, nullsFirst);
    }

    public static Comparator<Object[]> arrayOrdering(List<? extends DataType<?>> rowTypes,
                                                     int[] positions,
                                                     boolean[] reverse,
                                                     boolean[] nullsFirst) {
        assert rowTypes.size() >= positions.length : "Must have a type for each order by position";
        if (positions.length == 1) {
            int position = positions[0];
            return arrayOrdering(rowTypes.get(position), position, reverse[0], nullsFirst[0]);
        }
        List<Comparator<Object[]>> comparators = new ArrayList<>(positions.length);
        for (int i = 0, positionLength = positions.length; i < positionLength; i++) {
            int position = positions[i];
            comparators.add(arrayOrdering(rowTypes.get(position), position, reverse[i], nullsFirst[i]));
        }
        return CompoundOrdering.of(comparators);
    }

    @SuppressWarnings("unchecked")
    public static <T> Comparator<Object[]> arrayOrdering(DataType<T> type, int position, boolean reverse, boolean nullsFirst) {
        return new NullAwareComparator<>(cells -> (T) cells[position], type, reverse, nullsFirst);
    }
}

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

package io.crate.operation.projectors.sorting;

import com.google.common.collect.Ordering;
import io.crate.analyze.OrderBy;
import io.crate.data.Row;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.execution.dsl.phases.RoutedCollectPhase;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public abstract class OrderingByPosition<T> extends Ordering<T> {

    public static Ordering<Object[]> arrayOrdering(RoutedCollectPhase collectPhase) {
        OrderBy orderBy = collectPhase.orderBy();
        assert orderBy != null : "collectPhase must have an orderBy clause to generate an ordering";
        return arrayOrdering(
            OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectPhase.toCollect()),
            orderBy.reverseFlags(),
            orderBy.nullsFirst()
        );
    }

    public static Ordering<Row> rowOrdering(PositionalOrderBy orderBy) {
        return rowOrdering(orderBy.indices(), orderBy.reverseFlags(), orderBy.nullsFirst());
    }

    public static Ordering<Row> rowOrdering(int[] positions, boolean[] reverseFlags, Boolean[] nullsFirst) {
        List<Comparator<Row>> comparators = new ArrayList<>(positions.length);
        for (int i = 0; i < positions.length; i++) {
            OrderingByPosition<Row> rowOrdering = OrderingByPosition.rowOrdering(
                positions[i], reverseFlags[i], nullsFirst[i]);
            comparators.add(rowOrdering.reverse());
        }
        return Ordering.compound(comparators);
    }

    public static OrderingByPosition<Row> rowOrdering(int position, boolean reverse, Boolean nullsFirst) {
        return new RowOrdering(position, reverse, nullsFirst);
    }

    private static class RowOrdering extends OrderingByPosition<Row> {

        public RowOrdering(int position, boolean reverse, Boolean nullsFirst) {
            super(position, reverse, nullsFirst);
        }

        @Override
        public int compare(@Nullable Row left, @Nullable Row right) {
            Comparable l = left != null ? (Comparable) left.get(position) : null;
            Comparable r = right != null ? (Comparable) right.get(position) : null;
            return ordering.compare(l, r);
        }
    }

    public static Ordering<Object[]> arrayOrdering(int[] position, boolean[] reverse, Boolean[] nullsFirst) {
        if (position.length == 1) {
            return arrayOrdering(position[0], reverse[0], nullsFirst[0]);
        }

        // TODO: if the reverse/nullsFirst flags are the same for all positions this could be optimized
        // to use just one single "inner" Ordering instance that is re-used for all positions
        List<Comparator<Object[]>> comparators = new ArrayList<>(position.length);
        for (int i = 0, positionLength = position.length; i < positionLength; i++) {
            comparators.add(arrayOrdering(position[i], reverse[i], nullsFirst[i]));
        }
        return Ordering.compound(comparators);
    }

    public static OrderingByPosition<Object[]> arrayOrdering(int position, boolean reverse, Boolean nullsFirst) {
        return new ArrayOrdering(position, reverse, nullsFirst);
    }

    private static class ArrayOrdering extends OrderingByPosition<Object[]> {

        private ArrayOrdering(int position, boolean reverse, Boolean nullsFirst) {
            super(position, reverse, nullsFirst);
        }

        @Override
        public int compare(@Nullable Object[] left, @Nullable Object[] right) {
            Comparable l = left != null ? (Comparable) left[position] : null;
            Comparable r = right != null ? (Comparable) right[position] : null;
            return ordering.compare(l, r);
        }
    }

    protected final int position;
    protected final Ordering<Comparable> ordering;

    private OrderingByPosition(int position, boolean reverse, @Nullable Boolean nullFirst) {
        this.position = position;

        // note, that we are reverse for the queue so this conditional is by intent
        Ordering<Comparable> ordering;
        nullFirst = nullFirst != null ? !nullFirst : null; // swap because queue is reverse
        if (reverse) {
            ordering = Ordering.natural();
            if (nullFirst == null || !nullFirst) {
                ordering = ordering.nullsLast();
            } else {
                ordering = ordering.nullsFirst();
            }
        } else {
            ordering = Ordering.natural().reverse();
            if (nullFirst == null || nullFirst) {
                ordering = ordering.nullsFirst();
            } else {
                ordering = ordering.nullsLast();
            }
        }
        this.ordering = ordering;
    }
}

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

import com.google.common.collect.Ordering;
import io.crate.analyze.OrderBy;
import io.crate.data.Row;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.consumer.OrderByPositionVisitor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class OrderingByPosition {

    public static Comparator<Object[]> arrayOrdering(RoutedCollectPhase collectPhase) {
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

    public static Ordering<Row> rowOrdering(int[] positions, boolean[] reverseFlags, boolean[] nullsFirst) {
        List<Comparator<Row>> comparators = new ArrayList<>(positions.length);
        for (int i = 0; i < positions.length; i++) {
            Comparator<Row> rowOrdering = rowOrdering(positions[i], reverseFlags[i], nullsFirst[i]);
            comparators.add(rowOrdering);
        }
        return Ordering.compound(comparators);
    }

    public static Comparator<Row> rowOrdering(int position, boolean reverse, boolean nullsFirst) {
        return new NullAwareComparator<>(row -> (Comparable) row.get(position), reverse, nullsFirst);
    }

    public static Comparator<Object[]> arrayOrdering(int[] position, boolean[] reverse, boolean[] nullsFirst) {
        if (position.length == 1) {
            return arrayOrdering(position[0], reverse[0], nullsFirst[0]);
        }
        List<Comparator<Object[]>> comparators = new ArrayList<>(position.length);
        for (int i = 0, positionLength = position.length; i < positionLength; i++) {
            comparators.add(arrayOrdering(position[i], reverse[i], nullsFirst[i]));
        }
        return Ordering.compound(comparators);
    }

    public static Comparator<Object[]> arrayOrdering(int position, boolean reverse, boolean nullsFirst) {
        return new NullAwareComparator<>(cells -> (Comparable) cells[position], reverse, nullsFirst);
    }
}

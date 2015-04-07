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
import io.crate.core.collections.Row;

import javax.annotation.Nullable;

public abstract class OrderingByPosition<T> extends Ordering<T> {

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

    private OrderingByPosition (int position, boolean reverse, @Nullable Boolean nullFirst) {
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

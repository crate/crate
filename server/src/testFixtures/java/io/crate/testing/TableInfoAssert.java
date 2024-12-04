/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.testing;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.util.Comparator;
import java.util.function.Function;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;

public class TableInfoAssert extends AbstractAssert<TableInfoAssert, TableInfo> {


    protected TableInfoAssert(TableInfo actual) {
        super(actual, TableInfoAssert.class);
    }

    public TableInfoAssert hasSize(int size) {
        Assertions.assertThat(actual).hasSize(size);
        return this;
    }

    public <K extends Comparable<K>> TableInfoAssert isSortedBy(final Function<Reference, K> extractSortingKeyFunction) {
        return isSortedBy(extractSortingKeyFunction, false, null);
    }

    public <K extends Comparable<K>> TableInfoAssert hasColsSortedBy(final Function<Reference, K> extractSortingKeyFunction) {
        refsSortedBy(actual.columns(), extractSortingKeyFunction, false, null);
        return this;
    }

    public <K extends Comparable<K>> TableInfoAssert isSortedBy(final Function<Reference, K> extractSortingKeyFunction,
                                                                final boolean descending,
                                                                @Nullable final Boolean nullsFirst) {
        describedAs("expected iterable to be sorted " +
            (descending ? "in DESCENDING order" : "in ASCENDING order"));
        refsSortedBy(actual, extractSortingKeyFunction, descending, nullsFirst);
        return this;
    }

    private static <K extends Comparable<K>> void refsSortedBy(Iterable<Reference> refs,
                                                               final Function<Reference, K> extractSortingKeyFunction,
                                                               final boolean descending,
                                                               @Nullable final Boolean nullsFirst) {

        Comparator<K> comparator = Comparator.naturalOrder();
        if (descending) {
            comparator = Comparator.reverseOrder();
        }
        if (nullsFirst != null && nullsFirst) {
            comparator = Comparator.nullsFirst(comparator);
        } else {
            comparator = Comparator.nullsLast(comparator);
        }

        K previous = null;
        int i = 0;
        for (Reference elem : refs) {
            K current = extractSortingKeyFunction.apply(elem);
            if (previous != null) {
                assertThat(comparator.compare(previous, current))
                    .as("element '%s' at position %s is %s than previous element '%s'",
                        current, i, descending ? "bigger" : "smaller", previous)
                    .isLessThanOrEqualTo(0);
            }
            i++;
            previous = current;
        }
    }
}

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

import java.util.Comparator;
import java.util.function.Function;

/**
 * Specialized comparator implementation that directly handles reverse/nullsFirst.
 * (`Comparator.comparing()` doesn't handle nulls)
 *
 * We favour this over chaining Comparator.nullsFirst/nullsLast + .reversed()
 * Because sorting is often one of the main bottlenecks and we want to have a small call stack for that.
 */
class NullAwareComparator<T> implements Comparator<T> {

    private final int mod;
    private final Function<T, Comparable<Object>> keyExtractor;
    private final int leftNull;
    private final int rightNull;

    NullAwareComparator(Function<T, Comparable<Object>> keyExtractor, boolean reverse, boolean nullsFirst) {
        this.keyExtractor = keyExtractor;
        this.mod = reverse ? -1 : 1;
        this.leftNull = nullsFirst ? -1 : 1;
        this.rightNull = nullsFirst ? 1 : -1;
    }

    @Override
    public int compare(T o1, T o2) {
        var val1 = keyExtractor.apply(o1);
        var val2 = keyExtractor.apply(o2);
        if (val1 == val2) {
            return 0;
        }
        if (val1 == null) {
            return leftNull;
        }
        if (val2 == null) {
            return rightNull;
        }
        int cmp = val1.compareTo(val2);
        return cmp * mod;
    }
}

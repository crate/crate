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

package io.crate.common.collections;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


public final class CompoundOrdering<T> implements Comparator<T> {

    final Comparator<? super T>[] comparators;

    @SuppressWarnings("unchecked")
    public static final <T> Comparator<T> of(List<? extends Comparator<? super T>> comparators) {
        if (comparators.size() == 1) {
            return (Comparator<T>) comparators.get(0);
        }
        return new CompoundOrdering<>(comparators);
    }

    @SuppressWarnings("unchecked")
    private CompoundOrdering(List<? extends Comparator<? super T>> comparators) {
        this.comparators = comparators.toArray(Comparator[]::new);
    }

    @Override
    public int compare(T left, T right) {
        for (int i = 0; i < comparators.length; i++) {
            int result = comparators[i].compare(left, right);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof CompoundOrdering<?> other) {
            return Arrays.equals(this.comparators, other.comparators);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(comparators);
    }

    @Override
    public String toString() {
        return "Ordering.compound(" + Arrays.toString(comparators) + ")";
    }
}

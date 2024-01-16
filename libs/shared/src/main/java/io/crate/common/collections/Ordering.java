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


import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;

import org.jetbrains.annotations.Nullable;


/*
 * Extracted from https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/Ordering.java
 */
public abstract class Ordering<T> implements Comparator<T> {

    /**
     * Constructs a new instance of this class (only invokable by the subclass constructor, typically
     * implicit).
     */
    protected Ordering() {
    }

    /**
     * Returns an ordering which tries each given comparator in order until a non-zero result is
     * found, returning that result, and returning zero only if all comparators return zero. The
     * returned ordering is based on the state of the {@code comparators} iterable at the time it was
     * provided to this method.
     *
     * <p>The returned ordering is equivalent to that produced using {@code
     * Ordering.from(comp1).compound(comp2).compound(comp3) . . .}.
     *
     * <p><b>Warning:</b> Supplying an argument with undefined iteration order, such as a {@link
     * HashSet}, will produce non-deterministic results.
     *
     * <p><b>Java 8 users:</b> Use a chain of calls to {@link Comparator#thenComparing(Comparator)},
     * or {@code comparatorCollection.stream().reduce(Comparator::thenComparing).get()} (if the
     * collection might be empty, also provide a default comparator as the {@code identity} parameter
     * to {@code reduce}).
     *
     * @param comparators the comparators to try in order
     */
    public static <T> Ordering<T> compound(Iterable<? extends Comparator<? super T>> comparators) {
        return new CompoundOrdering<T>(comparators);
    }

    // Regular instance methods

    // Override to add @Nullable
    @Override
    public abstract int compare(@Nullable T left, @Nullable T right);

    // Never make these public
    static final int LEFT_IS_GREATER = 1;
    static final int RIGHT_IS_GREATER = -1;


    static final class CompoundOrdering<T> extends Ordering<T> implements Serializable {
        final Comparator<? super T>[] comparators;

        @SuppressWarnings("unchecked")
        CompoundOrdering(Iterable<? extends Comparator<? super T>> comparators) {
            this.comparators = Iterables.toArray(comparators, new Comparator[0]);
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
            if (object == this) {
                return true;
            }
            if (object instanceof CompoundOrdering) {
                CompoundOrdering<?> that = (CompoundOrdering<?>) object;
                return Arrays.equals(this.comparators, that.comparators);
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

        private static final long serialVersionUID = 0;
    }
}

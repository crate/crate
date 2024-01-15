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
import java.util.Iterator;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;


/*
 * Extracted from https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/Ordering.java
 */
public abstract class Ordering<T> implements Comparator<T> {

    /**
     * Returns a serializable ordering that uses the natural order of the values. The ordering throws
     * a {@link NullPointerException} when passed a null parameter.
     *
     * <p>The type specification is {@code <C extends Comparable>}, instead of the technically correct
     * {@code <C extends Comparable<? super C>>}, to support legacy types from before Java 5.
     *
     * <p><b>Java 8 users:</b> use {@link Comparator#naturalOrder} instead.
     */
    public static <C extends Comparable> Ordering<C> natural() {
        return (Ordering<C>) NaturalOrdering.INSTANCE;
    }


    /**
     * Returns an ordering that compares objects by the natural ordering of their string
     * representations as returned by {@code toString()}. It does not support null values.
     *
     * <p>The comparator is serializable.
     *
     * <p><b>Java 8 users:</b> Use {@code Comparator.comparing(Object::toString)} instead.
     */
    public static Ordering<Object> usingToString() {
        return UsingToStringOrdering.INSTANCE;
    }


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

    /**
     * Returns a new ordering which sorts iterables by comparing corresponding elements pairwise until
     * a nonzero result is found; imposes "dictionary order". If the end of one iterable is reached,
     * but not the other, the shorter iterable is considered to be less than the longer one. For
     * example, a lexicographical natural ordering over integers considers {@code [] < [1] < [1, 1] <
     * [1, 2] < [2]}.
     *
     * <p>Note that {@code ordering.lexicographical().reverse()} is not equivalent to {@code
     * ordering.reverse().lexicographical()} (consider how each would order {@code [1]} and {@code [1,
     * 1]}).
     *
     * <p><b>Java 8 users:</b> Use {@link Comparators#lexicographical(Comparator)} instead.
     *
     */
    // type parameter <S> lets us avoid the extra <String> in statements like:
    // Ordering<Iterable<String>> o =
    //     Ordering.<String>natural().lexicographical();
    public <S extends T> Ordering<Iterable<S>> lexicographical() {
        /*
         * Note that technically the returned ordering should be capable of
         * handling not just {@code Iterable<S>} instances, but also any {@code
         * Iterable<? extends S>}. However, the need for this comes up so rarely
         * that it doesn't justify making everyone else deal with the very ugly
         * wildcard.
         */
        return new LexicographicalOrdering<S>(this);
    }

    // Regular instance methods

    // Override to add @Nullable
    @Override
    public abstract int compare(@Nullable T left, @Nullable T right);

    // Never make these public
    static final int LEFT_IS_GREATER = 1;
    static final int RIGHT_IS_GREATER = -1;


    static final class NaturalOrdering extends Ordering<Comparable> implements Serializable {
        static final NaturalOrdering INSTANCE = new NaturalOrdering();

        @Override
        public int compare(Comparable left, Comparable right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            return left.compareTo(right);
        }

        // preserving singleton-ness gives equals()/hashCode() for free
        private Object readResolve() {
            return INSTANCE;
        }

        @Override
        public String toString() {
            return "Ordering.natural()";
        }

        private NaturalOrdering() {
        }

        private static final long serialVersionUID = 0;
    }


    static final class CompoundOrdering<T> extends Ordering<T> implements Serializable {
        final Comparator<? super T>[] comparators;

        CompoundOrdering(Comparator<? super T> primary, Comparator<? super T> secondary) {
            this.comparators = (Comparator<? super T>[]) new Comparator[] {primary, secondary};
        }

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

    static final class LexicographicalOrdering<T> extends Ordering<Iterable<T>> implements Serializable {
        final Comparator<? super T> elementOrder;

        LexicographicalOrdering(Comparator<? super T> elementOrder) {
            this.elementOrder = elementOrder;
        }

        @Override
        public int compare(Iterable<T> leftIterable, Iterable<T> rightIterable) {
            Iterator<T> left = leftIterable.iterator();
            Iterator<T> right = rightIterable.iterator();
            while (left.hasNext()) {
                if (!right.hasNext()) {
                    return LEFT_IS_GREATER; // because it's longer
                }
                int result = elementOrder.compare(left.next(), right.next());
                if (result != 0) {
                    return result;
                }
            }
            if (right.hasNext()) {
                return RIGHT_IS_GREATER; // because it's longer
            }
            return 0;
        }

        @Override
        public boolean equals(@Nullable Object object) {
            if (object == this) {
                return true;
            }
            if (object instanceof LexicographicalOrdering) {
                LexicographicalOrdering<?> that = (LexicographicalOrdering<?>) object;
                return this.elementOrder.equals(that.elementOrder);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return elementOrder.hashCode() ^ 2075626741; // meaningless
        }

        @Override
        public String toString() {
            return elementOrder + ".lexicographical()";
        }

        private static final long serialVersionUID = 0;
    }

    static final class UsingToStringOrdering extends Ordering<Object> implements Serializable {
        static final UsingToStringOrdering INSTANCE = new UsingToStringOrdering();

        @Override
        public int compare(Object left, Object right) {
            return left.toString().compareTo(right.toString());
        }

        // preserve singleton-ness, so equals() and hashCode() work correctly
        private Object readResolve() {
            return INSTANCE;
        }

        @Override
        public String toString() {
            return "Ordering.usingToString()";
        }

        private UsingToStringOrdering() {
        }

        private static final long serialVersionUID = 0;
    }

}

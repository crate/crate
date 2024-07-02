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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jetbrains.annotations.Nullable;

import io.crate.common.io.Streams;

/**
 * Extracted from https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/Iterables.java
 */
public final class Iterables {

    private Iterables() {
    }

    /**
     * Returns the single element contained in {@code iterable}.
     *
     * <p><b>Java 8 users:</b> the {@code Stream} equivalent to this method is {@code
     * stream.collect(MoreCollectors.onlyElement())}.
     *
     * @throws NoSuchElementException if the iterable is empty
     * @throws IllegalArgumentException if the iterable contains multiple elements
     */
    public static <T> T getOnlyElement(Iterable<T> iterable) {
        return Iterators.getOnlyElement(iterable.iterator());
    }

    /**
     * Returns the first element in {@code iterable} or {@code defaultValue} if the iterable is empty.
     * The {@link Iterators} analog to this method is {@link Iterators#getNext}.
     *
     * <p>If no default value is desired (and the caller instead wants a {@link
     * NoSuchElementException} to be thrown), it is recommended that {@code
     * iterable.iterator().next()} is used instead.
     *
     * <p>To get the only element in a single-element {@code Iterable}, consider using {@link
     * #getOnlyElement(Iterable)} or {@link #getOnlyElement(Iterable, Object)} instead.
     *
     * <p><b>{@code Stream} equivalent:</b> {@code stream.findFirst().orElse(defaultValue)}
     *
     * @param defaultValue the default value to return if the iterable is empty
     * @return the first element of {@code iterable} or the default value
     */
    public static <T> T getFirst(Iterable<? extends T> iterable, @Nullable T defaultValue) {
        return Iterators.getNext(iterable.iterator(), defaultValue);
    }


    /**
     * Returns the last element of {@code iterable} or {@code defaultValue} if the iterable is empty.
     * If {@code iterable} is a {@link List} with {@link RandomAccess} support, then this operation is
     * guaranteed to be {@code O(1)}.
     *
     * <p><b>{@code Stream} equivalent:</b> {@code Streams.findLast(stream).orElse(defaultValue)}
     *
     * @param defaultValue the value to return if {@code iterable} is empty
     * @return the last element of {@code iterable} or the default value
     */
    @Nullable
    public static <T> T getLast(Iterable<? extends T> iterable, @Nullable T defaultValue) {
        if (iterable instanceof Collection) {
            Collection<? extends T> c = (Collection<? extends T>) iterable;
            if (c.isEmpty()) {
                return defaultValue;
            } else if (iterable instanceof List) {
                return getLastInNonemptyList((List<? extends T>)iterable);
            }
        }
        return Iterators.getLast(iterable.iterator(), defaultValue);
    }

    /**
     * Returns the last element of {@code iterable}. If {@code iterable} is a {@link List} with {@link
     * java.util.RandomAccess} support, then this operation is guaranteed to be {@code O(1)}.
     *
     * <p><b>{@code Stream} equivalent:</b> {@link Streams#findLast Streams.findLast(stream).get()}
     *
     * @return the last element of {@code iterable}
     * @throws java.util.NoSuchElementException if the iterable is empty
     */
    public static <T> T getLast(Iterable<T> iterable) {
        if (iterable instanceof List) {
            List<T> list = (List<T>) iterable;
            if (list.isEmpty()) {
                throw new NoSuchElementException();
            }
            return getLastInNonemptyList(list);
        }

        return Iterators.getLast(iterable.iterator());
    }

    private static <T> T getLastInNonemptyList(List<T> list) {
        return list.get(list.size() - 1);
    }

    /**
     * Combines multiple iterables into a single iterable. The returned iterable has an iterator that
     * traverses the elements of each iterable in {@code inputs}. The input iterators are not polled
     * until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it. The methods of the returned iterable may throw {@code
     * NullPointerException} if any of the input iterators is null.
     *
     * <p><b>Java 8 users:</b> The {@code Stream} equivalent of this method is {@code
     * streamOfStreams.flatMap(s -> s)}.
     */
    public static <T> Iterable<T> concat(Iterable<? extends Iterable<? extends T>> inputs) {
        return FluentIterable.concat(inputs);
    }

    /**
     * Combines multiple iterables into a single iterable. The returned iterable has an iterator that
     * traverses the elements of each iterable in {@code inputs}. The input iterators are not polled
     * until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it.
     *
     * <p><b>Java 8 users:</b> The {@code Stream} equivalent of this method is {@code
     * Streams.concat(...)}.
     *
     * @throws NullPointerException if any of the provided iterables is null
     */
    @SafeVarargs
    public static final <T> Iterable<T> concat(Iterable<? extends T>... inputs) {
        return FluentIterable.concat(inputs);
    }


    /**
     * Combines two iterables into a single iterable. The returned iterable has an iterator that
     * traverses the elements in {@code a}, followed by the elements in {@code b}. The source
     * iterators are not polled until necessary.
     *
     * <p>The returned iterable's iterator supports {@code remove()} when the corresponding input
     * iterator supports it.
     *
     * <p><b>Java 8 users:</b> The {@code Stream} equivalent of this method is {@code Stream.concat(a,
     * b)}.
     */
    public static <T> Iterable<T> concat(Iterable<? extends T> a, Iterable<? extends T> b) {
        return FluentIterable.concat(a, b);
    }


    /**
     * Divides an iterable into unmodifiable sublists of the given size (the final iterable may be
     * smaller). For example, partitioning an iterable containing {@code [a, b, c, d, e]} with a
     * partition size of 3 yields {@code [[a, b, c], [d, e]]} -- an outer iterable containing two
     * inner lists of three and two elements, all in the original order.
     *
     * <p>Iterators returned by the returned iterable do not support the {@link Iterator#remove()}
     * method. The returned lists implement {@link RandomAccess}, whether or not the input list does.
     *
     * <p><b>Note:</b> if {@code iterable} is a {@link List}, use {@link Lists#partition(List, int)}
     * instead.
     *
     * @param iterable the iterable to return a partitioned view of
     * @param size the desired size of each partition (the last may be smaller)
     * @return an iterable of unmodifiable lists containing the elements of {@code iterable} divided
     *     into partitions
     * @throws IllegalArgumentException if {@code size} is nonpositive
     */
    public static <T> Iterable<List<T>> partition(final Iterable<T> iterable, final int size) {
        Objects.requireNonNull(iterable);
        assert (size > 0);
        return () -> Iterators.partition(iterable.iterator(), size);
    }


    /**
     * Returns a view containing the result of applying {@code function} to each element of {@code
     * fromIterable}.
     *
     * <p>The returned iterable's iterator supports {@code remove()} if {@code fromIterable}'s
     * iterator does. After a successful {@code remove()} call, {@code fromIterable} no longer
     * contains the corresponding element.
     *
     * <p>If the input {@code Iterable} is known to be a {@code List} or other {@code Collection},
     * consider {@link Lists#transform} and {@link Collections2#transform}.
     *
     * <p><b>{@code Stream} equivalent:</b> {@link Stream#map}
     */
    public static <F, T> Iterable<T> transform(final Iterable<F> fromIterable, final Function<? super F, ? extends T> function) {
        Objects.requireNonNull(fromIterable);
        Objects.requireNonNull(function);
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                return Iterators.transform(fromIterable.iterator(), function);
            }

            @Override
            public void forEach(Consumer<? super T> action) {
                Objects.requireNonNull(action);
                fromIterable.forEach((F f) -> action.accept(function.apply(f)));
            }

            @Override
            public Spliterator<T> spliterator() {
                return Iterators.map(fromIterable.spliterator(), function);
            }
        };
    }

    /**
     * Returns an iterable over the merged contents of all given {@code iterables}. Equivalent entries
     * will not be de-duplicated.
     *
     * <p>Callers must ensure that the source {@code iterables} are in non-descending order as this
     * method does not sort its input.
     *
     * <p>For any equivalent elements across all {@code iterables}, it is undefined which element is
     * returned first.
     *
     */
    public static <T> Iterable<T> mergeSorted(final Iterable<? extends Iterable<? extends T>> iterables, final Comparator<? super T> comparator) {
        Objects.requireNonNull(iterables, "iterables");
        Objects.requireNonNull(comparator, "comparator");
        return () -> Iterators.mergeSorted(Iterables.transform(iterables, Iterable::iterator), comparator);
    }

    /**
     * Adds all elements in {@code iterable} to {@code collection}.
     *
     * @return {@code true} if {@code collection} was modified as a result of this operation.
     */
    public static <T> boolean addAll(Collection<T> addTo, Iterable<? extends T> elementsToAdd) {
        if (elementsToAdd instanceof Collection) {
            Collection<? extends T> c = (Collection<? extends T>) elementsToAdd;
            return addTo.addAll(c);
        }
        return Iterators.addAll(addTo, Objects.requireNonNull(elementsToAdd).iterator());
    }

    abstract static class FluentIterable<E> implements Iterable<E> {
        // We store 'iterable' and use it instead of 'this' to allow Iterables to perform instanceof
        // checks on the _original_ iterable when FluentIterable.from is used.
        // To avoid a self retain cycle under j2objc, we store Optional.absent() instead of
        // Optional.of(this). To access the iterator delegate, call #getDelegate(), which converts to
        // absent() back to 'this'.
        private final Optional<Iterable<E>> iterableDelegate;

        /** Constructor for use by subclasses. */
        protected FluentIterable() {
            this.iterableDelegate = Optional.empty();
        }

        FluentIterable(Iterable<E> iterable) {
            Objects.requireNonNull(iterable);
            this.iterableDelegate = Optional.ofNullable(this != iterable ? iterable : null);
        }

        public static <T> FluentIterable<T> concat(Iterable<? extends T> a, Iterable<? extends T> b) {
            return concatNoDefensiveCopy(a, b);
        }

        @SafeVarargs
        public static final <T> FluentIterable<T> concat(Iterable<? extends T>... inputs) {
            return concatNoDefensiveCopy(Arrays.copyOf(inputs, inputs.length));
        }

        public static <T> FluentIterable<T> concat(
            final Iterable<? extends Iterable<? extends T>> inputs) {
            Objects.requireNonNull(inputs);
            return new FluentIterable<T>() {
                @Override
                public Iterator<T> iterator() {
                    return Iterators.concat(Iterators.transform(inputs.iterator(), Iterable::iterator));
                }
            };
        }

        @SafeVarargs
        private static final <T> FluentIterable<T> concatNoDefensiveCopy(final Iterable<? extends T>... inputs) {
            for (Iterable<? extends T> input : inputs) {
                Objects.requireNonNull(input);
            }
            return new FluentIterable<T>() {
                @Override
                public Iterator<T> iterator() {
                    return Iterators.concat(
                        /* lazily generate the iterators on each input only as needed */
                        new Iterators.AbstractIndexedListIterator<Iterator<? extends T>>(inputs.length) {
                            @Override
                            public Iterator<? extends T> get(int i) {
                                return inputs[i].iterator();
                            }
                        });
                }
            };
        }

        @Override
        public String toString() {
            return iterableDelegate.toString();
        }

    }
}

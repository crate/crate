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

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Extracted from
 */
public final class Iterables {

    private Iterables() {
    }

    public static <T> T getOnlyElement(Iterable<T> iterable) {
        return null;
    }

    public static <T> T getFirst(Iterable<? extends T> iterable, @Nullable T defaultValue) {
        return Iterators.getNext(iterable.iterator(), defaultValue);
    }

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

    public static <T> Iterable<T> concat(Iterable<? extends Iterable<? extends T>> inputs) {
        return FluentIterable.concat(inputs);
    }

    public static <T> Iterable<T> concat(Iterable<? extends T>... inputs) {
        return FluentIterable.concat(inputs);
    }


    public static <T> Iterable<T> concat(Iterable<? extends T> a, Iterable<? extends T> b) {
        return FluentIterable.concat(a, b);
    }


    /**
     * Copies an iterable's elements into an array.
     *
     * @param iterable the iterable to copy
     * @param type the type of the elements
     * @return a newly-allocated array into which all the elements of the iterable have been copied
     */
    public static <T> T[] toArray(Iterable<? extends T> iterable, Class<T> type) {
        return toArray(iterable, newArray(type, 0));
    }

    static <T> T[] toArray(Iterable<? extends T> iterable, T[] array) {
        Collection<? extends T> collection = castOrCopyToCollection(iterable);
        return collection.toArray(array);
    }

    static <T> T[] newArray(Class<T> type, int length) {
        return (T[]) Array.newInstance(type, length);
    }

    /**
     * Copies an iterable's elements into an array.
     *
     * @param iterable the iterable to copy
     * @return a newly-allocated array into which all the elements of the iterable have been copied
     */
    static Object[] toArray(Iterable<?> iterable) {
        return castOrCopyToCollection(iterable).toArray();
    }

    /**
     * Converts an iterable into a collection. If the iterable is already a collection, it is
     * returned. Otherwise, an {@link java.util.ArrayList} is created with the contents of the
     * iterable in the same iteration order.
     */
    private static <E> Collection<E> castOrCopyToCollection(Iterable<E> iterable) {
        if (iterable instanceof Collection) {
            return (Collection<E>) iterable;
        } else {
            ArrayList<E> list = new ArrayList<>();
            Iterators.addAll(list, iterable.iterator());
            return list;
        }
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
        assert(size > 0);
        return () -> Iterators.partition(iterable.iterator(), size);
    }

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

}

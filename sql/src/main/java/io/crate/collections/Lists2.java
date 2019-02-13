/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.collections;

import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class Lists2 {

    private Lists2() {
    }

    /**
     * Create a new list that contains the elements of both arguments
     */
    public static <T> List<T> concat(Collection<? extends T> list1, Collection<? extends T> list2) {
        ArrayList<T> list = new ArrayList<>(list1.size() + list2.size());
        list.addAll(list1);
        list.addAll(list2);
        return list;
    }

    public static <T> List<T> concat(Collection<? extends T> list1, T item) {
        ArrayList<T> xs = new ArrayList<>(list1.size() + 1);
        xs.addAll(list1);
        xs.add(item);
        return xs;
    }

    public static <T> List<T> concatUnique(List<? extends T> list1, List<? extends T> list2) {
        List<T> result = new ArrayList<>(list1.size() + list2.size());
        result.addAll(list1);
        for (T item : list2) {
            if (!list1.contains(item)) {
                result.add(item);
            }
        }
        return result;
    }

    /**
     * Apply the replace function on each item of the list and replaces the item.
     *
     * This is similar to {@link Lists#transform(List, com.google.common.base.Function)}, but instead of creating a
     * view on a backing list this function is actually mutating the provided list
     */
    public static <T> void mutate(@Nullable List<T> list, Function<? super T, ? extends T> mapper) {
        if (list == null || list.isEmpty()) {
            return;
        }
        ListIterator<T> it = list.listIterator();
        while (it.hasNext()) {
            it.set(mapper.apply(it.next()));
        }
    }

    /**
     * Create a copy of the given list with {@code mapper} applied on each item.
     * Opposed to {@link java.util.stream.Stream#map(Function)} / {@link Collectors#toList()} this minimizes allocations.
     */
    public static <I, O> List<O> map(Collection<I> list, Function<? super I, ? extends O> mapper) {
        List<O> copy = new ArrayList<>(list.size());
        for (I item : list) {
            copy.add(mapper.apply(item));
        }
        return copy;
    }

    /**
     * Return the first element of a list or raise an IllegalArgumentException if there are more than 1 items.
     *
     * Similar to {@link com.google.common.collect.Iterables#getOnlyElement(Iterable)}, but avoids an iterator allocation
     *
     * @throws NoSuchElementException If the list is empty
     * @throws IllegalArgumentException If the list has more than 1 element
     */
    public static <T> T getOnlyElement(List<T> items) {
        switch (items.size()) {
            case 0:
                throw new NoSuchElementException("List is empty");

            case 1:
                return items.get(0);

            default:
                throw new IllegalArgumentException("Expected 1 element, got: " + items.size());
        }
    }
}

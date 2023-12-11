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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Sets {

    private Sets() {

    }

    public static <T> Set<T> newConcurrentHashSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    public static <T> boolean haveEmptyIntersection(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        return left.stream().noneMatch(right::contains);
    }

    @SafeVarargs
    public static <T> Set<T> concat(Set<T> left, T ... right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        var result = new HashSet<T>(left);
        result.addAll(List.of(right));
        return Collections.unmodifiableSet(result);
    }

    public static <T> Set<T> union(Set<T> left, Set<T> right) {
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
        Set<T> union = new HashSet<>(left);
        union.addAll(right);
        return union;
    }

    public static <T> Set<T> intersection(Set<T> set1, Set<T> set2) {
        Objects.requireNonNull(set1);
        Objects.requireNonNull(set2);
        final Set<T> left;
        final Set<T> right;
        if (set1.size() < set2.size()) {
            left = set1;
            right = set2;
        } else {
            left = set2;
            right = set1;
        }
        return left.stream().filter(right::contains).collect(Collectors.toSet());
    }

    public static <E> Set<E> difference(final Set<E> set1, final Set<?> set2) {
        Objects.requireNonNull(set1, "set1");
        Objects.requireNonNull(set2, "set2");

        return new AbstractSet<E>() {
            @Override
            public Iterator<E> iterator() {
                return new AbstractIterator<E>() {
                    final Iterator<E> itr = set1.iterator();

                    @Override
                    protected E computeNext() {
                        while (itr.hasNext()) {
                            E e = itr.next();
                            if (!set2.contains(e)) {
                                return e;
                            }
                        }
                        return endOfData();
                    }
                };
            }

            @Override
            public Stream<E> stream() {
                return set1.stream().filter(e -> !set2.contains(e));
            }

            @Override
            public Stream<E> parallelStream() {
                return set1.parallelStream().filter(e -> !set2.contains(e));
            }

            @Override
            public int size() {
                int size = 0;
                for (E e : set1) {
                    if (!set2.contains(e)) {
                        size++;
                    }
                }
                return size;
            }

            @Override
            public boolean isEmpty() {
                return set2.containsAll(set1);
            }

            @Override
            public boolean contains(Object element) {
                return set1.contains(element) && !set2.contains(element);
            }
        };
    }
}

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
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;


public class Sets {

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

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

import org.jspecify.annotations.Nullable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.RandomAccess;

/*
 * Based on https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/CartesianList.java
 */
public final class CartesianList<E> extends AbstractList<List<E>> implements RandomAccess {

    private final transient List<List<E>> axes;
    private final transient int[] axesSizeProduct;

    public static <E> List<List<E>> of(List<? extends List<? extends E>> lists) {
        var axes = new ArrayList<List<E>>(lists.size());
        for (var list : lists) {
            // Cartesian product is empty if one of the sets are empty
            if (list.isEmpty()) {
                return List.of();
            }
            axes.add(List.copyOf(list));
        }
        return new CartesianList<E>(axes);
    }

    CartesianList(List<List<E>> axes) {
        this.axes = axes;
        int[] axesSizeProduct = new int[axes.size() + 1];
        axesSizeProduct[axes.size()] = 1;
        for (int i = axes.size() - 1; i >= 0; i--) {
            long result = (long) axesSizeProduct[i + 1] * axes.get(i).size();
            assert result == (int) result : "Cartesian product too large; must have size at most Integer.MAX_VALUE";
            axesSizeProduct[i] = (int) result;
        }
        this.axesSizeProduct = axesSizeProduct;
    }

    private int getAxisIndexForProductIndex(int index, int axis) {
        return (index / axesSizeProduct[axis + 1]) % axes.get(axis).size();
    }

    @Override
    public int indexOf(Object o) {
        if (!(o instanceof List)) {
            return -1;
        }
        List<?> list = (List<?>) o;
        if (list.size() != axes.size()) {
            return -1;
        }
        ListIterator<?> itr = list.listIterator();
        int computedIndex = 0;
        while (itr.hasNext()) {
            int axisIndex = itr.nextIndex();
            int elemIndex = axes.get(axisIndex).indexOf(itr.next());
            if (elemIndex == -1) {
                return -1;
            }
            computedIndex += elemIndex * axesSizeProduct[axisIndex + 1];
        }
        return computedIndex;
    }

    @Override
    public List<E> get(final int index) {
        Objects.checkIndex(index, size());
        return new AbstractList<E>() {

            @Override
            public int size() {
                return axes.size();
            }

            @Override
            public E get(int axis) {
                Objects.checkIndex(axis, size());
                int axisIndex = getAxisIndexForProductIndex(index, axis);
                return axes.get(axis).get(axisIndex);
            }
        };
    }

    @Override
    public int size() {
        return axesSizeProduct[0];
    }

    @Override
    public boolean contains(@Nullable Object o) {
        return indexOf(o) != -1;
    }
}

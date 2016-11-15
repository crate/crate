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

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class Lists2 {

    public static <T> List<T> concatUnique(List<T> list1, List<T> list2) {
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
     * This is similar to {@link Lists#transform(List, Function)}, but instead of creating a view on a backing list
     * this function is actually mutating the provided list
     */
    public static <T> void replaceItems(@Nullable List<T> list, Function<? super T, T> replaceFunction) {
        if (list == null || list.isEmpty()) {
            return;
        }
        ListIterator<T> it = list.listIterator();
        while (it.hasNext()) {
            it.set(replaceFunction.apply(it.next()));
        }
    }

    public static <T> List<T> copyAndReplace(List<T> list, Function<? super T, T> replaceFunc) {
        List<T> copy = new ArrayList<T>(list.size());
        for (T item : list) {
            copy.add(replaceFunc.apply(item));
        }
        return copy;
    }
}

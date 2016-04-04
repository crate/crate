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

package io.crate.core.collections;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.base.Function;

import java.util.List;
import java.util.ListIterator;

public class IntMaps {

    public static <TItem, KType> ObjectIntMap<KType> uniqueIndex(List<TItem> items, Function<? super TItem, KType> keyFunction) {
        // TODO: return empty map singleton if items isEmpty

        ObjectIntMap<KType> map = new ObjectIntOpenHashMap<>(items.size(), 1);
        ListIterator<TItem> it = items.listIterator();
        while (it.hasNext()) {
            map.put(keyFunction.apply(it.next()), it.previousIndex());
        }
        return map;
    }
}

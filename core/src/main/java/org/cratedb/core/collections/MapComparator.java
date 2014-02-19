/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.core.collections;

import com.google.common.base.Preconditions;

import java.util.Map;

public class MapComparator {

    public static <K, V> int compareMaps(Map<K, V> m1, Map<K, V> m2) {
        Preconditions.checkNotNull(m1, "map is null");
        Preconditions.checkNotNull(m2, "map is null");
        int sizeCompare = Integer.valueOf(m1.size()).compareTo(m2.size());
        if (sizeCompare != 0)
            return sizeCompare;
        for (K key: m1.keySet()) {
            if (!m1.get(key).equals(m2.get(key)))
                return 1;
        }
        return 0;
    }

    public static <K, V extends Comparable<V>> int compareMapsComparable(Map<K, V> m1, Map<K, V> m2) {
        Preconditions.checkNotNull(m1, "map is null");
        Preconditions.checkNotNull(m2, "map is null");
        int sizeCompare = Integer.valueOf(m1.size()).compareTo(m2.size());
        if (sizeCompare != 0)
            return sizeCompare;
        int valueCompare;
        for (K key: m1.keySet()) {
            valueCompare = m1.get(key).compareTo(m2.get(key));
            if (valueCompare != 0) {
                return valueCompare;
            }
        }
        return 0;
    }
}

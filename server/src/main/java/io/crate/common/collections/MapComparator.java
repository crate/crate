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

import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class MapComparator implements Comparator<Map> {

    private static final MapComparator INSTANCE = new MapComparator();

    private MapComparator() {
    }

    public static MapComparator getInstance() {
        return INSTANCE;
    }

    public static <K, V> int compareMaps(Map<K, V> m1, Map<K, V> m2) {
        Objects.requireNonNull(m1, "map is null");
        Objects.requireNonNull(m2, "map is null");
        int sizeCompare = Integer.compare(m1.size(), m2.size());
        if (sizeCompare != 0) {
            return sizeCompare;
        }
        for (Map.Entry<K, V> entry : m1.entrySet()) {
            V thisValue = entry.getValue();
            V otherValue = m2.get(entry.getKey());
            if (thisValue == null) {
                if (otherValue != null) {
                    return 1;
                } else {
                    continue;
                }
            }
            if (!thisValue.equals(otherValue)) {
                if (otherValue == null) {
                    return -1;
                }
                if (!thisValue.getClass().equals(otherValue.getClass())) {
                    DataType leftType = DataTypes.guessType(thisValue);
                    int cmp = leftType.compare(
                        thisValue,
                        leftType.implicitCast(otherValue)
                    );
                    if (cmp == 0) {
                        continue;
                    }
                    return cmp;
                }
                return 1;
            }
        }
        return 0;
    }

    @Override
    public int compare(Map o1, Map o2) {
        return compareMaps(o1, o2);
    }
}

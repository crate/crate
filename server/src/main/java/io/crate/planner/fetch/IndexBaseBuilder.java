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

package io.crate.planner.fetch;

import com.carrotsearch.hppc.IntIndexedContainer;
import com.carrotsearch.hppc.cursors.IntCursor;

import java.util.Map;
import java.util.TreeMap;

public final class IndexBaseBuilder {

    private final TreeMap<String, Integer> baseByIndex = new TreeMap<>();

    public void allocate(String index, IntIndexedContainer shards) {
        if (shards.isEmpty()) {
            return;
        }
        Integer currentMax = baseByIndex.get(index);
        int newMax = getMax(shards);
        if (currentMax == null || currentMax < newMax) {
            baseByIndex.put(index, newMax);
        }
    }

    public TreeMap<String, Integer> build() {
        int currentBase = 0;
        for (Map.Entry<String, Integer> entry : baseByIndex.entrySet()) {
            Integer maxId = entry.getValue();
            entry.setValue(currentBase);
            currentBase += maxId + 1;
        }
        return baseByIndex;
    }

    private static int getMax(IntIndexedContainer shards) {
        int max = -1;
        for (IntCursor shard: shards) {
            if (shard.value > max) {
                max = shard.value;
            }
        }
        return max;
    }
}

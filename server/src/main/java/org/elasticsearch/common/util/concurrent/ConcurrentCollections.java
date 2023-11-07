/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public abstract class ConcurrentCollections {

    static final int AGGRESSIVE_CONCURRENCY_LEVEL;

    static {
        AGGRESSIVE_CONCURRENCY_LEVEL = Math.max(Runtime.getRuntime().availableProcessors() * 2, 16);
    }

    /**
     * Creates a new CHM with an aggressive concurrency level, aimed at high concurrent update rate long living maps.
     */
    public static <K, V> ConcurrentMap<K, V> newConcurrentMapWithAggressiveConcurrency() {
        return newConcurrentMapWithAggressiveConcurrency(16);
    }

    /**
     * Creates a new CHM with an aggressive concurrency level, aimed at high concurrent update rate long living maps.
     */
    public static <K, V> ConcurrentMap<K, V> newConcurrentMapWithAggressiveConcurrency(int initalCapacity) {
        return new ConcurrentHashMap<>(initalCapacity, 0.75f, AGGRESSIVE_CONCURRENCY_LEVEL);
    }

    public static <V> Set<V> newConcurrentSet() {
        return Collections.newSetFromMap(new ConcurrentHashMap<>());
    }

    private ConcurrentCollections() {

    }
}

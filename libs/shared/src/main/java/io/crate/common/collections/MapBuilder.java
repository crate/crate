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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public final class MapBuilder<K, V> {

    public static <K, V> MapBuilder<K, V> newMapBuilder() {
        return new MapBuilder<>();
    }

    public static <K, V> MapBuilder<K, V> newMapBuilder(Map<K, V> map) {
        return new MapBuilder<>(new HashMap<>(map));
    }

    public static <K, V> MapBuilder<K, V> newLinkedHashMapBuilder() {
        return new MapBuilder<K, V>(new LinkedHashMap<K, V>());
    }

    public static <K extends Comparable<?>, V> MapBuilder<K, V> treeMapBuilder() {
        return new MapBuilder<>(new TreeMap<>());
    }

    private final Map<K, V> map;

    private MapBuilder() {
        this.map = new HashMap<>();
    }

    private MapBuilder(Map<K, V> map) {
        this.map = map;
    }

    public MapBuilder<K, V> put(K key, V value) {
        this.map.put(key, value);
        return this;
    }

    public Map<K, V> map() {
        return this.map;
    }

    public Map<K, V> immutableMap() {
        return Collections.unmodifiableMap(map);
    }

    public void putAll(Map<K, V> value) {
        map.putAll(value);
    }

    /// like [#put(Object, Object)] but fails if the entry already exists in the map.
    ///
    /// @param context name of the component/map. Used in the error message.
    public MapBuilder<K, V> putUnique(String context, K key, V value) {
        V prev = map.putIfAbsent(key, value);
        if (prev != null) {
            throw new IllegalArgumentException(context + ": name [" + key + "] already registered");
        }
        return this;
    }

    /// Adds all entries from all `maps` using [#putUnique(String, Object, Object)]
    public MapBuilder<K, V> putUnique(String context, Iterable<? extends Map<K, ? extends V>> maps) {
        for (var map : maps) {
            for (var entry : map.entrySet()) {
                putUnique(context, entry.getKey(), entry.getValue());
            }
        }
        return this;
    }
}

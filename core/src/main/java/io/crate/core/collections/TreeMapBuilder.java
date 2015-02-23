/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.core.collections;

import java.util.Map;

import static com.google.common.collect.Maps.newTreeMap;

public class TreeMapBuilder<K extends Comparable, V> {

    public static <K extends Comparable, V> TreeMapBuilder<K, V> newMapBuilder() {
        return new TreeMapBuilder<>();
    }

    private Map<K, V> map = newTreeMap();

    public TreeMapBuilder() {
        this.map = newTreeMap();
    }

    public TreeMapBuilder<K, V> putAll(Map<K, V> map) {
        this.map.putAll(map);
        return this;
    }

    public TreeMapBuilder<K, V> put(K key, V value) {
        this.map.put(key, value);
        return this;
    }

    public TreeMapBuilder<K, V> remove(K key) {
        this.map.remove(key);
        return this;
    }

    public TreeMapBuilder<K, V> clear() {
        this.map.clear();
        return this;
    }

    public V get(K key) {
        return map.get(key);
    }

    public boolean containsKey(K key) {
        return map.containsKey(key);
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Map<K, V> map() {
        return this.map;
    }
}

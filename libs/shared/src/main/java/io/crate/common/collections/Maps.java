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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import io.crate.common.StringUtils;
import io.crate.common.TriConsumer;

public final class Maps {

    public static <K, V> Map<K, V> concat(Map<K, V> m1, Map<K, V> m2) {
        if (m1.isEmpty()) {
            return m2;
        }
        if (m2.isEmpty()) {
            return m1;
        }
        HashMap<K, V> result = new HashMap<>();
        result.putAll(m1);
        result.putAll(m2);
        return Collections.unmodifiableMap(result);
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(Map<String, ?> map, String key) {
        return (T) map.get(key);
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOrDefault(@Nullable Map<String, Object> map, String key, T defaultValue) {
        if (map == null) {
            return defaultValue;
        }
        return (T) map.getOrDefault(key, defaultValue);
    }

    @Nullable
    public static Object getByPath(Map<String, Object> map, String path) {
        assert path != null : "path should not be null";
        return getByPath(map, StringUtils.splitToList('.', path));
    }

    @Nullable
    public static Object getByPath(Map<String, Object> value, List<String> path) {
        assert path instanceof RandomAccess : "Path must support random access for fast iteration";
        Map<String, Object> map = value;
        for (int i = 0; i < path.size(); i++) {
            String key = path.get(i);
            Object val = map.get(key);
            if (i + 1 == path.size()) {
                return val;
            } else if (val instanceof Map<?, ?>) {
                //noinspection unchecked
                map = (Map<String, Object>) val;
            } else {
                return null;
            }
        }
        return map;
    }

    @Nullable
    public static <T> T removeByPath(Map<String, T> map, List<String> path) {
        assert path instanceof RandomAccess : "`path` must support random access for performance";
        Map<String, T> m = map;
        for (int i = 0; i < path.size(); i++) {
            String key = path.get(i);
            if (i + 1 == path.size()) {
                return m.remove(key);
            } else {
                T val = map.get(key);
                if (val instanceof Map) {
                    m = (Map<String, T>) val;
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Inserts a value into source under the given key+path
     */
    public static void mergeInto(Map<String, Object> source,
                                 String key,
                                 List<String> path,
                                 Object value) {
        mergeInto(source, key, path, value, Map::put);
    }

    /**
     * Inserts a value into source under the given key+path
     */
    public static void mergeInto(Map<String, Object> source,
                                 String key,
                                 List<String> path,
                                 Object value,
                                 TriConsumer<Map<String, Object>, String, Object> writer) {
        if (path.isEmpty()) {
            writer.accept(source, key, value);
        } else {
            if (source.containsKey(key)) {
                Map<String, Object> contents = (Map<String, Object>) source.get(key);
                if (contents == null) {
                    contents = new HashMap<>();
                    source.put(key, contents);
                }
                String nextKey = path.get(0);
                mergeInto(contents, nextKey, path.subList(1, path.size()), value, writer);
            } else {
                writer.accept(source, key, nestedMaps(path, value));
            }
        }
    }

    private static Map<String, Object> nestedMaps(List<String> path, Object value) {
        final HashMap<String, Object> root = new HashMap<>(1);
        HashMap<String, Object> m = root;
        for (int i = 0, size = path.size(); i < size; i++) {
            String key = path.get(i);
            if (i + 1 == size) {
                m.put(key, value);
            } else {
                HashMap<String, Object> nextChild = new HashMap<>(1);
                m.put(key, nextChild);
                m = nextChild;
            }
        }
        return root;
    }


    /**
     * Add entries in `additions` to `map`.
     * If `map` already contains an entry that is also present in `addition`:
     *  - It will recurse into it if it is a map
     *  - Skip the entry otherwise
     **/
    public static void extendRecursive(Map<String, Object> map, Map<String, Object> additions) {
        extendRecursive(map, additions, (oldList, newList) -> Lists2.concatUnique(oldList, newList));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void extendRecursive(Map<String, Object> map,
                                       Map<String, Object> additions,
                                       BiFunction<List<Object>, Collection<Object>, List<Object>> mergeLists) {
        for (Map.Entry<String, Object> additionEntry : additions.entrySet()) {
            String key = additionEntry.getKey();
            Object addition = additionEntry.getValue();
            if (map.containsKey(key)) {
                Object sourceValue = map.get(key);
                if (sourceValue instanceof Map && addition instanceof Map) {
                    //noinspection unchecked
                    extendRecursive((Map) sourceValue, (Map) addition);
                }
                if (sourceValue instanceof List && addition instanceof Collection) {
                    map.put(key, mergeLists.apply((List<Object>) sourceValue, (List<Object>) addition));
                }
            } else {
                map.put(key, addition);
            }
        }
    }
}

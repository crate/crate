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

package io.crate.common.collections;

import io.crate.common.StringUtils;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.RandomAccess;

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

    @Nullable
    public static <K, V> Map<K, V> mapOrNullIfNullValues(Map<K, V> map) {
        return map.values().stream().anyMatch(Objects::nonNull) ? map : null;
    }

    @SuppressWarnings("unchecked")
    public static <T> T get(Map map, String key) {
        return (T) map.get(key);
    }

    public static <T> T getOrDefault(@Nullable Map map, String key, T defaultValue) {
        if (map == null) {
            return defaultValue;
        }
        //noinspection unchecked
        return (T) map.getOrDefault(key, defaultValue);
    }

    @Nullable
    public static Object getByPath(Map<String, Object> map, String path) {
        assert path != null : "path should not be null";
        return getByPath(map, StringUtils.PATH_SPLITTER.splitToList(path));
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
            } else if (val instanceof Map) {
                //noinspection unchecked
                map = (Map<String, Object>) val;
            } else {
                return null;
            }
        }
        return map;
    }

    /**
     * Inserts a value into source under the given key+path
     */
    public static void mergeInto(Map<String, Object> source, String key, List<String> path, Object value) {
        if (path.isEmpty()) {
            source.put(key, value);
        } else {
            if (source.containsKey(key)) {
                Map<String, Object> contents = (Map<String, Object>) source.get(key);
                if (contents == null) {
                    contents = new HashMap<>();
                    source.put(key, contents);
                }
                String nextKey = path.get(0);
                mergeInto(contents, nextKey, path.subList(1, path.size()), value);
            } else {
                source.put(key, nestedMaps(path, value));
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
}

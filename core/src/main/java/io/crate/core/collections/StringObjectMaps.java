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

import io.crate.core.StringUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

public final class StringObjectMaps {

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
}

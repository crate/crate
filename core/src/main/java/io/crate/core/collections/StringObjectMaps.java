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
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Map;

public class StringObjectMaps {

    @Nullable
    public static Object getByPath(Map<String, Object> map, String path) {
        assert path != null : "path should not be null";
        Object tmp;
        List<String> splittedPath = StringUtils.PATH_SPLITTER.splitToList(path);
        for (String pathElement : splittedPath.subList(0, splittedPath.size() - 1)) {
            tmp = map.get(pathElement);
            if (tmp != null && tmp instanceof Map) {
                map = (Map<String, Object>) tmp;
            } else {
                break;
            }
        }

        if (map != null) {
            // get last path element
            return map.get(splittedPath.get(splittedPath.size() - 1));
        }
        return null;
    }

    public static Object fromMapByPath(Map value, List<String> path) {
        Map map = value;
        Object tmp = null;
        for (String s : path) {
            tmp = map.get(s);
            if (tmp instanceof Map) {
                map = (Map) tmp;
            } else {
                break;
            }
        }
        return tmp;
    }
}

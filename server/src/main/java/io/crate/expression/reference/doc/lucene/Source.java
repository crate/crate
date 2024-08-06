/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.reference.doc.lucene;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;

import io.crate.metadata.ColumnIdent;

public interface Source {

    Map<String, Object> sourceAsMap();

    String rawSource();

    default Object get(List<String> path) {
        return extractValue(sourceAsMap(), path, 0);
    }

    static Object extractValue(final Map<?, ?> map, ColumnIdent columnIdent) {
        List<String> fullPath = new ArrayList<>();
        fullPath.add(columnIdent.name());
        fullPath.addAll(columnIdent.path());
        return extractValue(map, fullPath, 0);
    }

    static Object extractValue(final Map<?, ?> map, List<String> path, int pathStartIndex) {
        assert path instanceof RandomAccess : "path should support RandomAccess for fast index optimized loop";
        Map<?, ?> m = map;
        Object tmp = null;
        for (int i = pathStartIndex; i < path.size(); i++) {
            tmp = m.get(path.get(i));
            if (tmp instanceof Map) {
                m = (Map<?, ?>) tmp;
            } else if (tmp instanceof List<?> list) {
                if (i + 1 == path.size()) {
                    return list;
                }
                ArrayList<Object> newList = new ArrayList<>(list.size());
                for (Object o : list) {
                    if (o instanceof Map) {
                        newList.add(extractValue((Map<?, ?>) o, path, i + 1));
                    } else {
                        newList.add(o);
                    }
                }
                return newList;
            } else {
                if (i + 1 != path.size()) {
                    return null;
                }
                break;
            }
        }
        return tmp;
    }
}

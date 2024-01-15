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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class Sorted {


    public static LinkedHashMap<String, Object> sortRecursive(Map<String, Object> map, boolean sortOnlyMaps) {
        LinkedHashMap<String, Object> sortedMap = new LinkedHashMap<>(map.size(), 1.0f);
        ArrayList<String> sortedKeys = new ArrayList<>(map.keySet());
        Collections.sort(sortedKeys);
        for (String sortedKey : sortedKeys) {
            Object o = map.get(sortedKey);
            if (o instanceof Map) {
                //noinspection unchecked
                sortedMap.put(sortedKey, sortRecursive((Map<String, Object>) o, sortOnlyMaps));
            } else if (o instanceof Collection) {
                sortedMap.put(sortedKey, sortOnlyMaps ? o : sortRecursive((Collection) o));
            } else {
                sortedMap.put(sortedKey, o);
            }
        }
        return sortedMap;
    }

    public static Collection sortRecursive(Collection collection) {
        if (collection.size() == 0) {
            return collection;
        }
        Object firstElement = collection.iterator().next();
        if (firstElement instanceof Map) {
            ArrayList sortedList = new ArrayList(collection.size());
            for (Object obj : collection) {
                //noinspection unchecked
                sortedList.add(sortRecursive((Map<String, Object>) obj, true));
            }
            Collections.sort(sortedList);
            return sortedList;
        }

        ArrayList sortedList = new ArrayList(collection);
        Collections.sort(sortedList);
        return sortedList;
    }
}

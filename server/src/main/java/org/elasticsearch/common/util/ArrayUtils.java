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

package org.elasticsearch.common.util;

import java.lang.reflect.Array;

public class ArrayUtils {

    private ArrayUtils() {
    }

    /**
     * Concatenates 2 arrays
     */
    public static String[] concat(String[] one, String[] other) {
        return concat(one, other, String.class);
    }

    /**
     * Concatenates 2 arrays
     */
    public static <T> T[] concat(T[] one, T[] other, Class<T> clazz) {
        T[] target = (T[]) Array.newInstance(clazz, one.length + other.length);
        System.arraycopy(one, 0, target, 0, one.length);
        System.arraycopy(other, 0, target, one.length, other.length);
        return target;
    }
}

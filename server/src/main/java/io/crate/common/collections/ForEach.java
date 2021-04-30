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

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.function.Consumer;

public class ForEach {

    /**
     * Invoke a consumer for each elements of a collection or an array
     *
     * @param arrayOrCollection a collection, a primitive or non-primitive array
     * @param consumer          called for every element of <code>arrayOrCollection</code> as Object
     */
    public static void forEach(Object arrayOrCollection, Consumer<Object> consumer) {
        if (arrayOrCollection.getClass().isArray()) {
            int arrayLength = Array.getLength(arrayOrCollection);
            for (int i = 0; i < arrayLength; i++) {
                Object elem = Array.get(arrayOrCollection, i);
                consumer.accept(elem);
            }
        } else if (arrayOrCollection instanceof Collection) {
            for (Object elem : ((Collection) arrayOrCollection)) {
                consumer.accept(elem);
            }
        } else {
            throw new AssertionError("argument is neither an array nor a collection");
        }
    }
}

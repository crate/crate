/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.core.collections;

import java.lang.reflect.Array;
import java.util.Collection;

public class ForEach {

    public static interface Acceptor {
        public void accept(Object value);
    }

    /**
     * apply an acceptor to all elements of a collection, a primitive or non-primitive array
     *
     * @param arrayOrCollection a collection, a primitive or non-primitive array
     * @param fun               called for every element of <code>arrayOrCollection</code> as Object
     */
    public static void forEach(Object arrayOrCollection, Acceptor fun) {
        if (arrayOrCollection.getClass().isArray()) {
            int arrayLength = Array.getLength(arrayOrCollection);
            for (int i = 0; i < arrayLength; i++) {
                Object elem = Array.get(arrayOrCollection, i);
                fun.accept(elem);
            }
        } else if (arrayOrCollection instanceof Collection) {
            for (Object elem : ((Collection) arrayOrCollection)) {
                fun.accept(elem);
            }
        } else {
            throw new AssertionError("argument is neither an array nor a collection");
        }
    }
}

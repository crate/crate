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

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.ObjectLookupContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import java.util.Iterator;

public final class HppcMaps {

    private HppcMaps() {
    }

    /**
     * @return an intersection view over the two specified containers (which can be KeyContainer or ObjectHashSet).
     */
    // Hppc has forEach, but this means we need to build an intermediate set, with this method we just iterate
    // over each unique value without creating a third set.
    public static <T> Iterable<T> intersection(ObjectLookupContainer<T> container1, final ObjectLookupContainer<T> container2) {
        assert container1 != null && container2 != null;
        final Iterator<ObjectCursor<T>> iterator = container1.iterator();
        final Iterator<T> intersection = new Iterator<T>() {

            T current;

            @Override
            public boolean hasNext() {
                if (iterator.hasNext()) {
                    do {
                        T next = iterator.next().value;
                        if (container2.contains(next)) {
                            current = next;
                            return true;
                        }
                    } while (iterator.hasNext());
                }
                return false;
            }

            @Override
            public T next() {
                return current;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return () -> intersection;
    }
}

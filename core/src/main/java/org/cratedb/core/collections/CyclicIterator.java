/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package org.cratedb.core.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that starts at the beginning when end of iterated collection is reached
 * @param <T>
 */
public class CyclicIterator<T> implements Iterator<T> {
    private final Collection<T> c;
    private Iterator<T> it;

    public CyclicIterator(Collection<T> c) {
        this.c = c;
        this.it = this.c.iterator();
    }

    public boolean hasNext() {
        return !c.isEmpty();
    }

    public T next() {
        T ret;

        if (!hasNext()) {
            throw new NoSuchElementException();
        } else if (it.hasNext()) {
            ret = it.next();
        } else {
            it = c.iterator();
            ret = it.next();
        }

        return ret;
    }

    public void remove() {
        it.remove();
    }
}

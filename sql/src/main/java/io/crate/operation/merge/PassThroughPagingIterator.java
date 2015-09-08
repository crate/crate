/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.merge;

import com.google.common.base.Function;
import com.google.common.collect.ForwardingIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

public class PassThroughPagingIterator<T> extends ForwardingIterator<T> implements PagingIterator<T> {

    private Iterator<T> iterator = Collections.emptyIterator();
    private final ImmutableList.Builder<Iterable<T>> iterables = ImmutableList.builder();
    private final boolean repeatable;
    private Iterable<T> storedForRepeat = null;

    private PassThroughPagingIterator(boolean repeatable) {
        this.repeatable = repeatable;
    }

    /**
     * Create an iterator that is able to repeat over what has previously been iterated
     */
    public static <T> PassThroughPagingIterator<T> repeatable() {
        return new PassThroughPagingIterator<>(true);
    }

    /**
     * Create an iterator that is not able to repeat.
     * Calling {@link #repeat()} with instances created by this method is discouraged.
     */
    public static <T> PassThroughPagingIterator<T> oneShot() {
        return new PassThroughPagingIterator<>(false);
    }

    @Override
    protected Iterator<T> delegate() {
        return iterator;
    }

    @Override
    public void merge(Iterable<? extends Iterable<T>> iterables) {
        if (repeatable) {
            this.iterables.addAll(iterables);
            this.storedForRepeat = null;
        }
        if (iterator.hasNext()) {
            iterator = Iterators.concat(iterator,
                    Iterables.concat(iterables).iterator());
        } else {
            iterator = Iterables.concat(iterables).iterator();
        }
    }

    @Override
    public void finish() {
    }

    @Override
    public Iterator<T> repeat() {
        Iterable<T> repeatMe = storedForRepeat;
        if (repeatMe == null) {
            repeatMe = Iterables.concat(this.iterables.build());
            this.storedForRepeat = repeatMe;
        }
        return repeatMe.iterator();
    }
}

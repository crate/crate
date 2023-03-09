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

package io.crate.execution.engine.distribution.merge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import io.crate.common.collections.Iterables;
import io.crate.common.collections.Iterators;

public class PassThroughPagingIterator<TKey, TRow> implements PagingIterator<TKey, TRow> {

    private Iterator<TRow> iterator = Collections.emptyIterator();
    private final ArrayList<KeyIterable<TKey, TRow>> iterables = new ArrayList<>();
    private final boolean repeatable;
    private Iterable<TRow> storedForRepeat = null;

    private PassThroughPagingIterator(boolean repeatable) {
        this.repeatable = repeatable;
    }

    /**
     * Create an iterator that is able to repeat over what has previously been iterated
     */
    public static <TKey, TRow> PassThroughPagingIterator<TKey, TRow> repeatable() {
        return new PassThroughPagingIterator<>(true);
    }

    /**
     * Create an iterator that is not able to repeat.
     * Calling {@link #repeat()} with instances created by this method is discouraged.
     */
    public static <TKey, TRow> PassThroughPagingIterator<TKey, TRow> oneShot() {
        return new PassThroughPagingIterator<>(false);
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public TRow next() {
        return iterator.next();
    }

    @Override
    public void merge(Iterable<? extends KeyIterable<TKey, TRow>> iterables) {
        Iterable<TRow> concat = Iterables.concat(iterables);

        if (repeatable) {
            for (var iterable : iterables) {
                this.iterables.add(iterable);
            }
            this.storedForRepeat = null;
        }
        if (iterator.hasNext()) {
            iterator = Iterators.concat(iterator, concat.iterator());
        } else {
            iterator = concat.iterator();
        }
    }

    @Override
    public void finish() {
    }

    @Override
    public TKey exhaustedIterable() {
        return null;
    }

    @Override
    public Iterable<TRow> repeat() {
        if (!repeatable) {
            throw new IllegalStateException("Can't repeat a non-repeatable iterator");
        }
        Iterable<TRow> repeatMe = storedForRepeat;
        if (repeatMe == null) {
            repeatMe = Iterables.concat(List.copyOf(this.iterables));
            this.storedForRepeat = repeatMe;
        }
        return repeatMe;
    }
}

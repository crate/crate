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


import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator that supports a one-element lookahead while iterating.
 * based on https://github.com/google/guava/blob/master/guava/src/com/google/common/collect/PeekingIterator.java   
 */
public interface PeekingIterator<E> extends Iterator<E> {
    /**
     * Returns the next element in the iteration, without advancing the iteration.
     *
     * <p>Calls to {@code peek()} should not change the state of the iteration, except that it
     * <i>may</i> prevent removal of the most recent element via {@link #remove()}.
     *
     * @throws NoSuchElementException if the iteration has no more elements according to {@link
     *     #hasNext()}
     */
    E peek();

    /**
     * {@inheritDoc}
     *
     * <p>The objects returned by consecutive calls to {@link #peek()} then {@link #next()} are
     * guaranteed to be equal to each other.
     */
    @Override
    E next();

    /**
     * {@inheritDoc}
     *
     * <p>Implementations may or may not support removal when a call to {@link #peek()} has occurred
     * since the most recent call to {@link #next()}.
     *
     * @throws IllegalStateException if there has been a call to {@link #peek()} since the most recent
     *     call to {@link #next()} and this implementation does not support this sequence of calls
     *     (optional)
     */
    @Override
    void remove();
}

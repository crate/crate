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

import java.util.Iterator;

public interface PagingIterator<TKey, TRow> extends Iterator<TRow> {

    /**
     * Add additional iterables to the PagingIterator. (E.g. due to a new Page that has arrived)
     */
    void merge(Iterable<? extends KeyIterable<TKey, TRow>> iterables);

    /**
     * This is called if the last page has been received and merge has been called for the last time.
     * If the PagingIterator implementation has been holding rows back, these rows should now be
     * returned on hasNext/next calls.
     */
    void finish();

    TKey exhaustedIterable();

    /**
     * create an iterable to repeat the previous iteration
     *
     * @return an iterable that will iterate through the already emitted items and emit them again in the same order as before
     */
    Iterable<TRow> repeat();
}

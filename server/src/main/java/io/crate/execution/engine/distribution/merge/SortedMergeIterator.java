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


import java.util.Iterator;

/**
 * MergingIterator like it is used in guava Iterators.mergedSort
 * It has (limited) shared object support.
 * <p>
 * And it also has a merge function with which additional backing iterables can be added to enable paging.
 */
public interface SortedMergeIterator<TKey, TRow> extends Iterator<TRow> {
    void merge(Iterable<? extends KeyIterable<TKey, TRow>> iterables);

    boolean isLeastExhausted();

    TKey exhaustedIterable();

    Iterable<TRow> repeat();
}

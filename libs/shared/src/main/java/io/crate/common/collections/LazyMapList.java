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

package io.crate.common.collections;

import java.util.AbstractList;
import java.util.List;
import java.util.RandomAccess;
import java.util.function.Function;

/**
 * {@code LazyMapList} is a wrapper around a list that lazily applies
 * the {@code mapper} {@code Function} on each item when it is accessed.
 */
public class LazyMapList<I, O> extends AbstractList<O> implements RandomAccess {

    private final List<I> list;
    private final Function<? super I, ? extends O> mapper;

    public static <I, O> LazyMapList<I, O> of(List<I> list, Function<? super I, ? extends O> mapper) {
        return new LazyMapList<>(list, mapper);
    }

    private LazyMapList(List<I> list, Function<? super I, ? extends O> mapper) {
        this.list = list;
        this.mapper = mapper;
    }

    @Override
    public O get(int index) {
        return mapper.apply(list.get(index));
    }

    @Override
    public int size() {
        return list.size();
    }
}

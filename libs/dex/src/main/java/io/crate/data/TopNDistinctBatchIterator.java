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

package io.crate.data;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

public final class TopNDistinctBatchIterator<T> implements BatchIterator<T> {

    private final BatchIterator<T> source;
    private final Set<T> items;
    private final int limit;
    private final Function<? super T, ? extends T> memoizeItem;

    public TopNDistinctBatchIterator(BatchIterator<T> source, int limit, Function<? super T, ? extends T> memoizeItem) {
        this.source = source;
        this.items = new HashSet<>();
        this.limit = limit;
        this.memoizeItem = memoizeItem;
    }

    @Override
    public void kill(Throwable throwable) {
        source.kill(throwable);
    }

    @Override
    public T currentElement() {
        return source.currentElement();
    }

    @Override
    public void moveToStart() {
        items.clear();
        source.moveToStart();
    }

    @Override
    public boolean moveNext() {
        if (items.size() == limit) {
            return false;
        }
        while (source.moveNext()) {
            if (items.contains(source.currentElement())) {
                continue;
            }
            T materializedItem = memoizeItem.apply(source.currentElement());
            boolean added = items.add(materializedItem);
            assert added : ".add must return true if .contains was false. source.currentElement() ("
                + source.currentElement()
                + " has a different hashCode/equals implementation than "
                + materializedItem;
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        source.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        return source.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return items.size() == limit || source.allLoaded();
    }

    @Override
    public boolean hasLazyResultSet() {
        return source.hasLazyResultSet();
    }
}

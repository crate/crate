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

import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

public final class FlatMapBatchIterator<TIn, TOut> implements BatchIterator<TOut> {

    private final BatchIterator<TIn> source;
    private final Function<TIn, ? extends Iterator<TOut>> mapper;

    private TOut current = null;
    private boolean onSourceRow = false;
    private Iterator<TOut> mappedElements;

    public FlatMapBatchIterator(BatchIterator<TIn> source, Function<TIn, ? extends Iterator<TOut>> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public TOut currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        current = null;
        mappedElements = null;
        onSourceRow = false;
        source.moveToStart();
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (onSourceRow) {
                if (mappedElements == null) {
                    mappedElements = mapper.apply(source.currentElement());
                }
                if (mappedElements.hasNext()) {
                    current = mappedElements.next();
                    return true;
                } else {
                    onSourceRow = false;
                    mappedElements = null;
                    // continue with source.moveNext()
                }
            } else {
                onSourceRow = source.moveNext();
                if (!onSourceRow) {
                    return false;
                }
            }
        }
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
        return source.allLoaded();
    }

    @Override
    public void kill(@NotNull Throwable throwable) {
        source.kill(throwable);
    }

    @Override
    public boolean isKilled() {
        return source.isKilled();
    }

    @Override
    public boolean hasLazyResultSet() {
        return source.hasLazyResultSet();
    }
}

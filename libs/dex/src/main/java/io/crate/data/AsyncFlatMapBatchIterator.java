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

import java.util.concurrent.CompletionStage;

import io.crate.common.exceptions.Exceptions;

public final class AsyncFlatMapBatchIterator<I, O> implements BatchIterator<O> {

    private final BatchIterator<I> source;
    private final AsyncFlatMapper<I, O> mapper;

    private NextAction nextAction = NextAction.SOURCE;
    private O current = null;
    private CloseableIterator<O> mappedElements;
    private boolean sourceExhausted = false;

    private enum NextAction {
        SOURCE,
        MAPPER,
    }

    public AsyncFlatMapBatchIterator(BatchIterator<I> source, AsyncFlatMapper<I, O> mapper) {
        this.source = source;
        this.mapper = mapper;
    }

    @Override
    public void kill(Throwable throwable) {
        source.kill(throwable);
    }

    @Override
    public O currentElement() {
        return current;
    }

    @Override
    public void moveToStart() {
        source.moveToStart();
        sourceExhausted = false;
        mappedElements = null;
        nextAction = NextAction.SOURCE;
        current = null;
    }

    @Override
    public boolean moveNext() {
        while (true) {
            if (nextAction == NextAction.SOURCE) {
                if (source.moveNext()) {
                    nextAction = NextAction.MAPPER;
                } else {
                    sourceExhausted = source.allLoaded();
                }
                return false;
            } else {
                if (mappedElements == null) {
                    // This is the case if a consumer didn't call loadNextBatch after a previous moveNext call returned false
                    return false;
                }
                if (mappedElements.hasNext()) {
                    current = mappedElements.next();
                    return true;
                } else {
                    // Make sure objects can be GCd early;
                    // Otherwise it would have to wait for the next loadNextBatch call+completion of the async operation
                    mappedElements.close();
                    mappedElements = null;

                    nextAction = NextAction.SOURCE;
                    continue;
                }
            }
        }
    }

    @Override
    public void close() {
        if (mappedElements != null) {
            mappedElements.close();
            mappedElements = null;
        }
        source.close();
        try {
            mapper.close();
        } catch (Exception e) {
            throw Exceptions.toRuntimeException(e);
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        if (nextAction == NextAction.SOURCE) {
            return source.loadNextBatch();
        } else {
            return mapper.apply(source.currentElement(), sourceExhausted).thenAccept(rows -> {
                mappedElements = rows;
            });
        }
    }

    @Override
    public boolean allLoaded() {
        return source.allLoaded() && nextAction == NextAction.SOURCE;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }
}

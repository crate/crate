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

package io.crate.data.join;

import java.util.concurrent.CompletionStage;

import org.jetbrains.annotations.NotNull;

import io.crate.data.BatchIterator;

/**
 * Basic class for BatchIterators that implement the various types of Joins.
 * <p>
 * It handles two Iterators the {@link #left} and {@link #right} corresponding to
 * the left and the right tables of a Join. It uses an element {@link #combiner} which
 * combines the data from the left and right iterators to produce the final output which
 * is passed to the consumer of this BatchIterator which gets it via the {@link #currentElement()}.
 * <p>
 * A helper {@link #activeIt} variable points to the currently active iterator thus making
 * transparent to the consumer of this BatchIterator, which calls the methods {@link #allLoaded()}
 * and {@link #loadNextBatch()}, which of the sides (left or right) are currently processed.
 * Usually those methods don't need to be overridden.
 * <p>
 * The methods {@link #close()} and {@link #kill(Throwable)} are closing or killing the
 * left and right iterators and usually don't need to be overridden.
 */
public abstract class JoinBatchIterator<L, R, C> implements BatchIterator<C> {

    protected final ElementCombiner<L, R, C> combiner;
    protected final BatchIterator<L> left;
    protected final BatchIterator<R> right;

    /**
     * points to the batchIterator which will be used on the next {@link #moveNext()} call
     */
    protected BatchIterator<?> activeIt;

    protected JoinBatchIterator(BatchIterator<L> left, BatchIterator<R> right, ElementCombiner<L, R, C> combiner) {
        this.left = left;
        this.right = right;
        this.activeIt = left;
        this.combiner = combiner;
    }

    @Override
    public C currentElement() {
        return combiner.currentElement();
    }

    @Override
    public void moveToStart() {
        left.moveToStart();
        right.moveToStart();
        activeIt = left;
    }

    boolean tryMoveLeft() {
        if (left.moveNext()) {
            combiner.setLeft(left.currentElement());
            return true;
        }
        return false;
    }

    boolean tryMoveRight() {
        if (right.moveNext()) {
            combiner.setRight(right.currentElement());
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        return activeIt.loadNextBatch();
    }

    @Override
    public boolean allLoaded() {
        return activeIt.allLoaded();
    }


    @Override
    public void kill(@NotNull Throwable throwable) {
        left.kill(throwable);
        right.kill(throwable);
    }

    @Override
    public boolean hasLazyResultSet() {
        return left.hasLazyResultSet() || right.hasLazyResultSet();
    }
}

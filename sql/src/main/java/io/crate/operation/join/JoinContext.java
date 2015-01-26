/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.join;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.RewindableIterator;
import io.crate.executor.PageInfo;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.Closeable;
import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * contains state needed during join execution
 * and must be portable between multiple execution steps
 */
class JoinContext implements Closeable {

    private final ESLogger logger = Loggers.getLogger(getClass());

    final RelationIterable outerIterable;
    final RelationIterable innerIterable;

    RewindableIterator<Object[]> outerIterator;
    RewindableIterator<Object[]> innerIterator;

    JoinContext(RelationIterable outerIterable,
                RelationIterable innerIterable) {
        this.outerIterable = outerIterable;
        this.innerIterable = innerIterable;
    }

    void refreshOuterIteratorIfNeeded(boolean force) {
        try {
            if (force || outerIterator == null || !outerIterator.hasNext()) {
                // outer iterator is iterated pagewise only
                outerIterator = outerIterable.forCurrentPage();
            }
        } catch (ConcurrentModificationException e) {
            // underlying list has been changed
            outerIterator = outerIterable.forCurrentPage();
        }
    }

    void refreshInnerIteratorIfNeeded() {
        try {
            if (innerIterator == null || !innerIterator.hasNext()) {
                innerIterator = innerIterable.rewindableIterator();
            }
        } catch (ConcurrentModificationException e) {
            // underlying list has been changed
            innerIterator = innerIterable.rewindableIterator();
        }
    }

    void refreshInnerIteratorToCurrentPage() {
        innerIterator = innerIterable.forCurrentPage();
    }

    ListenableFuture<Long> outerFetchNextPage(Optional<PageInfo> pageInfo) {
        logger.trace("fetching next {} rows from outer relation", pageInfo.or(outerIterable.currentPageInfo()).size());
        return fetchNextPage(pageInfo, outerIterable);
    }

    ListenableFuture<Long> innerFetchNextPage(Optional<PageInfo> pageInfo) {
        logger.trace("fetching next {} rows from inner relation", pageInfo.or(innerIterable.currentPageInfo()).size());
        return fetchNextPage(pageInfo, innerIterable);
    }

    static ListenableFuture<Long> fetchNextPage(Optional<PageInfo> pageInfo, RelationIterable iterable) {
        PageInfo nextPage;
        if (pageInfo.isPresent()) {
            // TODO: optimize/shrink nextPage size
            nextPage = iterable.currentPageInfo().nextPage(pageInfo.get().size());
        } else {
            nextPage = iterable.currentPageInfo().nextPage();
        }
        return iterable.fetchPage(nextPage);
    }

    boolean innerNeedsToFetchMore() {
        return !innerIterable.isComplete();
        //return innerIterator != null && !innerIterator.hasNext() && !innerIterable.isComplete();
    }

    boolean outerNeedsToFetchMore() {
        return outerIterator != null && !outerIterator.hasNext() && !outerIterable.isComplete();
    }

    @Override
    public void close() throws IOException {
        outerIterable.close();
        innerIterable.close();
    }
}

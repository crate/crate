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

package io.crate.executor.transport;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.bigarray.MultiNativeArrayBigArray;
import io.crate.core.bigarray.MultiObjectArrayBigArray;
import io.crate.executor.AbstractBigArrayPageableTaskResult;
import io.crate.executor.PageInfo;
import io.crate.executor.TaskResult;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.ObjectArray;

/**
 * {@linkplain io.crate.executor.TaskResult} that does not cache pages,
 * issues new queries when paging backwards or when the gap between two requested
 * pages is bigger than {@linkplain #maxGapSize()}.
 *
 * @param <T> the actual implementing class
 */
public abstract class AbstractNonCachingPageableTaskResult<T extends TaskResult> extends AbstractBigArrayPageableTaskResult {

    public static final ObjectArray<Object[]> EMPTY_PAGE_SOURCE = new MultiNativeArrayBigArray<Object[]>(0L, 0L, TaskResult.EMPTY_ROWS);

    protected final ESLogger logger = Loggers.getLogger(this.getClass());

    protected final PageInfo currentPageInfo;

    protected AbstractNonCachingPageableTaskResult(ObjectArray<Object[]> pageSource,
                                                   long startIndexAtPageSource,
                                                   PageInfo currentPageInfo) {
        super(pageSource, startIndexAtPageSource, currentPageInfo.size());
        this.currentPageInfo = currentPageInfo;
    }

    /**
     * fetch a new page from the data source
     */
    protected abstract void fetchFromSource(int from, int size, FutureCallback<ObjectArray<Object[]>> callback);

    /**
     * fetch a new page from the data source by issuing a new query.
     */
    protected abstract ListenableFuture<TaskResult> fetchWithNewQuery(PageInfo pageInfo);

    /**
     * if the gap between two pages + the requested pagesize is bigger than this
     * number, a new query is issued.
     *
     * only used when paging forward.
     */
    protected abstract int maxGapSize();

    /**
     * close and handle exceptions
     */
    protected abstract void closeSafe();

    /**
     * create a new taskresult from the given pagesource and pageinfo
     */
    protected abstract T newTaskResult(PageInfo pageInfo, ObjectArray<Object[]> pageSource, long startIndexAtPageSource);


    @Override
    public ListenableFuture<TaskResult> fetch(final PageInfo pageInfo) {
        final long pageSourceBeginning = currentPageInfo.position() - backingArrayStartIdx;
        if (pageInfo.position() < pageSourceBeginning) {
            logger.trace("paging backwards for page {}. issue new query", pageInfo);
            // backward paging - TODO: not optimized
            return fetchWithNewQuery(pageInfo);
        }
        final long pageSourceEnd = currentPageInfo.position() + (backingArray.size()-backingArrayStartIdx);
        final long gap = Math.max(0, pageInfo.position() - pageSourceEnd);

        final long restSize;
        if (gap == 0) {
            restSize = pageSourceEnd - pageInfo.position();
        } else {
            restSize = 0;
        }

        final SettableFuture<TaskResult> future = SettableFuture.create();
        if (restSize >= pageInfo.size()) {
            // don't need to fetch nuttin'
            logger.trace("can satisfy page {} with current page source", pageInfo);
            future.set(
                    newTaskResult(
                            pageInfo,
                            backingArray,
                            backingArrayStartIdx + (pageInfo.position() - currentPageInfo.position())
                    )
            );
        } else if (restSize <= 0) {
            if (gap + pageInfo.size() > maxGapSize()) {
                // if we have to fetch more than default pagesize, issue another query
                logger.trace("issue a new query for page {}. gap is too big.", pageInfo);
                return fetchWithNewQuery(pageInfo);
            } else {
                logger.trace("fetch another page: {}, we only got a small gap.", pageInfo);
                // make sure we fetch from source without gap
                fetchFromSource(
                        (int)(pageInfo.position() - gap),
                        (int)(pageInfo.size() + gap),
                        new FutureCallback<ObjectArray<Object[]>>() {
                            @Override
                            public void onSuccess(@Nullable ObjectArray<Object[]> result) {
                                if (result.size() == 0) {
                                    future.set(newTaskResult(
                                            new PageInfo(pageInfo.position(), 0), EMPTY_PAGE_SOURCE, 0L));
                                } else {
                                    future.set(
                                            newTaskResult(
                                                    pageInfo,
                                                    result,
                                                    Math.min(result.size(), gap)
                                            )
                                    );
                                }
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                closeSafe();
                                future.setException(t);
                            }
                        }
                );
            }
        } else if (restSize > 0) {
            logger.trace("we got {} docs left for page {}. need to fetch {}", restSize, pageInfo, pageInfo.size() - restSize);
            // we got a rest, need to combine stuff
            fetchFromSource(pageInfo.position(), (int)(pageInfo.size() - restSize), new FutureCallback<ObjectArray<Object[]>>() {
                @Override
                public void onSuccess(@Nullable ObjectArray<Object[]> result) {

                    MultiObjectArrayBigArray<Object[]> merged = new MultiObjectArrayBigArray<>(
                            0,
                            backingArray.size() + result.size(),
                            backingArray,
                            result
                    );
                    future.set(
                            newTaskResult(
                                    pageInfo,
                                    merged,
                                    backingArrayStartIdx + (pageInfo.position() - currentPageInfo.position())
                            )
                    );
                }

                @Override
                public void onFailure(Throwable t) {
                    closeSafe();
                    future.setException(t);
                }
            });
        }
        return future;
    }
}

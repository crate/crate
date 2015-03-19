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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.bigarray.MultiObjectArrayBigArray;
import io.crate.core.collections.Bucket;
import io.crate.executor.BigArrayPage;
import io.crate.executor.Page;
import io.crate.executor.PageInfo;
import io.crate.executor.PageableTaskResult;
import io.crate.operation.qtf.QueryThenFetchOperation;
import io.crate.planner.node.dql.ESQueryThenFetchNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;
import java.util.List;

/**
 * pageable taskresult used for paging through the results of a query then fetch
 * query
 *
 * keeps a reference on the query context
 *
 */
class QueryThenFetchPageableTaskResult implements PageableTaskResult {

    public static final int MAX_GAP_PAGESIZE = 1024;

    private static final ESLogger logger = Loggers.getLogger(QueryThenFetchPageableTaskResult.class);

    private final ObjectArray<Object[]> pageSource;
    private final Page page;
    private final long startIndexAtPageSource;
    private final PageInfo currentPageInfo;
    private final List<FieldExtractor<SearchHit>> extractors;

    private final QueryThenFetchOperation operation;
    private QueryThenFetchOperation.QueryThenFetchContext ctx;

    public QueryThenFetchPageableTaskResult(QueryThenFetchOperation operation,
                                            QueryThenFetchOperation.QueryThenFetchContext ctx,
                                            List<FieldExtractor<SearchHit>> extractors,
                                            PageInfo pageInfo,
                                            ObjectArray<Object[]> pageSource,
                                            long startIndexAtPageSource) {
        this.operation = operation;
        this.ctx = ctx;
        this.currentPageInfo = pageInfo;
        this.pageSource = pageSource;
        this.startIndexAtPageSource = startIndexAtPageSource;
        this.page = new BigArrayPage(pageSource, startIndexAtPageSource, pageInfo.size());
        this.extractors = extractors;
    }

    private void fetchFromSource(int from, int size, final FutureCallback<ObjectArray<Object[]>> callback) {
        Futures.addCallback(
                Futures.transform(operation.executePageQuery(from, size, ctx),
                        new Function<InternalSearchResponse, ObjectArray<Object[]>>() {
                            @javax.annotation.Nullable
                            @Override
                            public ObjectArray<Object[]> apply(@Nullable InternalSearchResponse input) {
                                return ctx.toPage(input.hits().hits(), extractors);
                            }
                        }),
                callback
        );
    }

    private ListenableFuture<PageableTaskResult> fetchWithNewQTF(final PageInfo pageInfo) {
        final SettableFuture<PageableTaskResult> future = SettableFuture.create();
        ESQueryThenFetchNode oldNode = ctx.searchNode();
        Futures.addCallback(
                operation.execute(
                        oldNode,
                        ctx.outputs(),
                        Optional.of(pageInfo)
                ),
                new FutureCallback<QueryThenFetchOperation.QueryThenFetchContext>() {
                    @Override
                    public void onSuccess(@Nullable final QueryThenFetchOperation.QueryThenFetchContext newCtx) {
                        Futures.addCallback(
                                newCtx.createSearchResponse(),
                                new FutureCallback<InternalSearchResponse>() {
                                    @Override
                                    public void onSuccess(@Nullable InternalSearchResponse searchResponse) {
                                        ObjectArray<Object[]> pageSource = newCtx.toPage(searchResponse.hits().hits(), extractors);
                                        newCtx.cleanAfterFirstPage();
                                        future.set(
                                                new QueryThenFetchPageableTaskResult(
                                                        operation,
                                                        newCtx,
                                                        extractors,
                                                        pageInfo,
                                                        pageSource,
                                                        0L
                                                )
                                        );
                                        closeSafe(); // close old searchcontexts and stuff
                                    }

                                    @Override
                                    public void onFailure(Throwable t) {
                                        closeSafe();
                                        future.setException(t);
                                    }
                                }
                        );
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        closeSafe();
                        future.setException(t);
                    }
                }
        );
        return future;
    }

    /**
     *
     * @param pageInfo identifying the page to fetch
     * @return a future for a new task result containing the requested page.
     */
    @Override
    public ListenableFuture<PageableTaskResult> fetch(final PageInfo pageInfo) {
        final long pageSourceBeginning = currentPageInfo.position() - startIndexAtPageSource;
        if (pageInfo.position() < pageSourceBeginning) {
            logger.trace("paging backwards for page {}. issue new QTF query", pageInfo);
            // backward paging - not optimized
            return fetchWithNewQTF(pageInfo);
        }
        final long pageSourceEnd = currentPageInfo.position() + (pageSource.size()-startIndexAtPageSource);
        final long gap = Math.max(0, pageInfo.position() - pageSourceEnd);

        final long restSize;
        if (gap == 0) {
            restSize = pageSourceEnd - pageInfo.position();
        } else {
            restSize = 0;
        }

        final SettableFuture<PageableTaskResult> future = SettableFuture.create();
        if (restSize >= pageInfo.size()) {
            // don't need to fetch nuttin'
            logger.trace("can satisfy page {} with current page source", pageInfo);
            future.set(
                    new QueryThenFetchPageableTaskResult(
                            operation,
                            ctx,
                            extractors,
                            pageInfo,
                            pageSource,
                            startIndexAtPageSource + (pageInfo.position() - currentPageInfo.position())
                    )
            );
        } else if (restSize <= 0) {
            if (gap + pageInfo.size() > MAX_GAP_PAGESIZE) {
                // if we have to fetch more than default pagesize, issue another query
                logger.trace("issue a new QTF query for page {}. gap is too big.", pageInfo);
                return fetchWithNewQTF(pageInfo);
            } else {
                logger.trace("fetch another page: {}, we only got a small gap.", pageInfo);
                fetchFromSource(
                        (int)(pageInfo.position() - gap),
                        (int)(pageInfo.size() + gap),
                        new FutureCallback<ObjectArray<Object[]>>() {
                            @Override
                            public void onSuccess(@Nullable ObjectArray<Object[]> result) {
                                future.set(
                                        new QueryThenFetchPageableTaskResult(
                                                operation,
                                                ctx,
                                                extractors,
                                                pageInfo,
                                                result,
                                                Math.min(result.size(), gap)
                                        )
                                );
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
                            pageSource.size() + result.size(),
                            pageSource,
                            result
                    );
                    future.set(
                            new QueryThenFetchPageableTaskResult(
                                    operation,
                                    ctx,
                                    extractors,
                                    pageInfo,
                                    merged,
                                    startIndexAtPageSource + (pageInfo.position() - currentPageInfo.position())
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

    @Override
    public Page page() {
        return page;
    }

    @Override
    public Bucket rows() {
        throw new UnsupportedOperationException("QTFScrollTaskResult does not support rows()");
    }

    @javax.annotation.Nullable
    @Override
    public String errorMessage() {
        return null;
    }

    @Override
    public void close() throws IOException {
        ctx.close();
    }

    private void closeSafe() {
        try {
            close();
        } catch (IOException e) {
            logger.error("error closing {}",e, getClass().getSimpleName());
        }
    }
}

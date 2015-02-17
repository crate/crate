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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.PageInfo;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.AbstractNonCachingPageableTaskResult;
import io.crate.operation.qtf.QueryThenFetchOperation;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchResponse;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * pageable taskresult used for paging through the results of a query then fetch
 * query
 *
 * keeps a reference on the query context
 *
 */
class QueryThenFetchPageableTaskResult extends AbstractNonCachingPageableTaskResult<QueryThenFetchPageableTaskResult> {

    public static final int MAX_GAP_PAGESIZE = 1024;

    private final List<FieldExtractor<SearchHit>> extractors;

    private final QueryThenFetchOperation operation;
    private QueryThenFetchOperation.QueryThenFetchContext ctx;

    public QueryThenFetchPageableTaskResult(QueryThenFetchOperation operation,
                                            QueryThenFetchOperation.QueryThenFetchContext ctx,
                                            List<FieldExtractor<SearchHit>> extractors,
                                            PageInfo pageInfo,
                                            ObjectArray<Object[]> pageSource,
                                            long startIndexAtPageSource) {
        super(pageSource, startIndexAtPageSource, pageInfo);
        this.operation = operation;
        this.ctx = ctx;
        this.extractors = extractors;
    }

    @Override
    protected void fetchFromSource(int from, int size, final FutureCallback<ObjectArray<Object[]>> callback) {
        operation.executePageQuery(from, size, ctx, new FutureCallback<InternalSearchResponse>(){

            @Override
            public void onSuccess(@Nullable InternalSearchResponse result) {
                callback.onSuccess(ctx.toPage(result.hits().hits(), extractors));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                callback.onFailure(t);
            }
        });
    }

    @Override
    protected ListenableFuture<TaskResult> fetchWithNewQuery(final PageInfo pageInfo) {
        final SettableFuture<TaskResult> future = SettableFuture.create();

        FutureCallback<QueryThenFetchOperation.QueryThenFetchContext> callback =
                new FutureCallback<QueryThenFetchOperation.QueryThenFetchContext>() {

            @Override
            public void onSuccess(@Nullable final QueryThenFetchOperation.QueryThenFetchContext newCtx) {
                newCtx.createSearchResponse(
                        new FutureCallback<InternalSearchResponse>() {
                            @Override
                            public void onSuccess(@Nullable InternalSearchResponse searchResponse) {
                                ObjectArray<Object[]> pageSource = newCtx.toPage(searchResponse.hits().hits(), extractors);
                                newCtx.cleanAfterFirstPage();
                                future.set(
                                        new QueryThenFetchPageableTaskResult(operation, newCtx, extractors, pageInfo, pageSource, 0L)
                                );
                                closeSafe(); // close old searchcontexts and stuff
                            }

                            @Override
                            public void onFailure(@Nonnull Throwable t) {
                                closeSafe();
                                future.setException(t);
                            }
                        }
                );
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                closeSafe();
                future.setException(t);
            }
        };
        operation.execute(callback, ctx.searchNode(), ctx.outputs(), Optional.of(pageInfo));
        return future;
    }

    @Override
    protected int maxGapSize() {
        return MAX_GAP_PAGESIZE;
    }

    @Override
    public void close() throws IOException {
        ctx.close();
    }

    @Override
    protected void closeSafe() {
        try {
            close();
        } catch (IOException e) {
            logger.error("error closing {}",e, getClass().getSimpleName());
        }
    }

    @Override
    protected QueryThenFetchPageableTaskResult newTaskResult(PageInfo pageInfo, ObjectArray<Object[]> pageSource, long startIndexAtPageSource) {
        return new QueryThenFetchPageableTaskResult(
                operation,
                ctx,
                extractors,
                pageInfo,
                pageSource,
                startIndexAtPageSource
        );
    }
}

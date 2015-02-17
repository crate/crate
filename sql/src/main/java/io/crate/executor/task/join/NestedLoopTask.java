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

package io.crate.executor.task.join;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.concurrent.ForwardingFutureCallback;
import io.crate.executor.*;
import io.crate.executor.pageable.CachingPageCache;
import io.crate.executor.pageable.NoOpPageCache;
import io.crate.executor.pageable.PageCache;
import io.crate.operation.join.nestedloop.JoinContext;
import io.crate.operation.join.nestedloop.NestedLoopExecutorService;
import io.crate.operation.join.nestedloop.NestedLoopOperation;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.dql.join.NestedLoopNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class NestedLoopTask extends JobTask implements PageableTask<JoinContext> {

    private final NestedLoopOperation operation;
    private final SettableFuture<TaskResult> result = SettableFuture.create();
    private final List<ListenableFuture<TaskResult>> results = Arrays.<ListenableFuture<TaskResult>>asList(result);

    private PageCache pageCache;

    private final ESLogger logger = Loggers.getLogger(getClass());

    public NestedLoopTask(UUID jobId,
                          String nodeId,
                          NestedLoopNode nestedLoopNode,
                          Job outerJob,
                          Job innerJob,
                          TaskExecutor executor,
                          NestedLoopExecutorService nestedLoopExecutorService,
                          ProjectionToProjectorVisitor projectionToProjectorVisitor,
                          CircuitBreaker circuitBreaker) {
        super(jobId);
        String ramContextId = String.format(Locale.ENGLISH, "%s: %s", nodeId, jobId.toString());
        RamAccountingContext ramAccountingContext = new RamAccountingContext(
                ramContextId,
                circuitBreaker);
        operation = new NestedLoopOperation(
                nestedLoopNode,
                nestedLoopExecutorService,
                outerJob.tasks(),
                innerJob.tasks(),
                executor,
                projectionToProjectorVisitor,
                ramAccountingContext
        );
        pageCache = NoOpPageCache.INSTANCE; // fallback
    }

    @Override
    public void start() {
        doStart(Optional.<PageInfo>absent(),
                new ForwardingFutureCallback<>(result));
    }

    @Override
    public void start(PageInfo pageInfo) {
        doStart(Optional.of(pageInfo),
                new ForwardingFutureCallback<>(result));
    }

    @Override
    public void startCached(final PageInfo pageInfo) {
        int cacheSize = Math.min(operation.limit(), 0) + operation.offset();
        pageCache = new CachingPageCache(cacheSize);
        logger.trace("using cache for paged NestedLoop execution with cache size of {}", cacheSize);
        doStart(Optional.of(pageInfo), new ForwardingFutureCallback<>(result));

    }

    private void doStart(final Optional<PageInfo> pageInfo, final FutureCallback<TaskResult> callback) {
        if (operation.limit() == 0) {
            callback.onSuccess(TaskResult.EMPTY_RESULT);
            return;
        }
        operation.execute(
                pageInfo,
                new FutureCallback<JoinContext>() {
                    @Override
                    public void onSuccess(@Nullable JoinContext joinContext) {
                        if (joinContext == null) {
                            result.setException(new NullPointerException("joinContext is null"));
                        } else {
                            // TODO: hide logic, which taskresult impl to use
                            if (!pageInfo.isPresent()) {
                                callback.onSuccess(new QueryResult(joinContext.joinedRows()));
                            } else {
                                if (operation.needsToFetchAllForPaging()) {
                                    // no need to cache here
                                    callback.onSuccess(FetchedRowsPageableTaskResult.forArray(joinContext.joinedRows(), 0, pageInfo.get()));
                                } else {
                                    PageInfo localPageInfo = pageInfo.get();
                                    Page page = new ObjectArrayPage(joinContext.joinedRows(), localPageInfo.position(), localPageInfo.size());
                                    pageCache.put(localPageInfo, page);
                                    callback.onSuccess(
                                            new PageableTaskResult<>(NestedLoopTask.this, localPageInfo, page, joinContext)
                                    );
                                }
                            }
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        callback.onFailure(t);
                    }
                }
        );
    }

    @Override
    public void fetchMore(final PageInfo pageInfo, final JoinContext context, final FutureCallback<TaskResult> callback) {
        Page cached = pageCache.get(pageInfo);
        if (cached != null) {
            callback.onSuccess(
                    new PageableTaskResult<>(this, pageInfo, cached, context)
            );
        } else {
            operation.fetchMoreRows(pageInfo, context, new FutureCallback<Object[][]>() {
                @Override
                public void onSuccess(@Nullable Object[][] rows) {
                    if (rows == null) {
                        callback.onFailure(new NullPointerException("NestedLoop result page is null"));
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("fetched {} new rows from NestedLoop for page {}.",
                                    rows.length, pageInfo);
                        }
                        Page page = new ObjectArrayPage(rows, 0, rows.length);
                        pageCache.put(pageInfo, page);
                        callback.onSuccess(
                                new PageableTaskResult<>(
                                        NestedLoopTask.this,
                                        pageInfo,
                                        page,
                                        context
                                )
                        );
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            });
        }
    }

    @Override
    public void fetchNew(PageInfo pageInfo, JoinContext context, FutureCallback<TaskResult> callback) {
        Page cached = pageCache.get(pageInfo);
        if (cached != null) {
            callback.onSuccess(
                    new PageableTaskResult<>(this, pageInfo, cached, context)
            );
        } else {
            try {
                context.close();
                if (operation.limit() != TopN.NO_LIMIT && operation.limit() - pageInfo.position() <= 0) {
                    callback.onSuccess(TaskResult.EMPTY_RESULT);
                }
                doStart(Optional.of(pageInfo), callback);
            } catch (IOException e) {
                callback.onFailure(e);
            }
        }
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        // ignore
    }
}

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

package io.crate.operation;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.Row;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * RowDownstream that will set a TaskResultFuture once the result is ready.
 * It will also close the associated context once it is done
 */
public class QueryResultRowDownstream implements RowDownstream {

    private final SettableFuture<TaskResult> result;
    private final UUID jobId;
    private final int executionNodeId;
    private final JobContextService jobContextService;

    public QueryResultRowDownstream(SettableFuture<TaskResult> result,
                                    UUID jobId,
                                    int executionNodeId,
                                    JobContextService jobContextService) {
        this.result = result;
        this.jobId = jobId;
        this.executionNodeId = executionNodeId;
        this.jobContextService = jobContextService;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        return new RowDownstreamHandle() {

            List<Object[]> rows = new ArrayList<>();

            @Override
            public boolean setNextRow(Row row) {
                rows.add(row.materialize());
                return true;
            }

            @Override
            public void finish() {
                result.set(new QueryResult(rows.toArray(new Object[rows.size()][])));
                closeContext();
            }

            @Override
            public void fail(Throwable throwable) {
                result.setException(throwable);
                closeContext();
            }

            private void closeContext() {
                JobExecutionContext context = jobContextService.getContext(jobId);
                if (context != null) {
                    context.closePageDownstreamContext(executionNodeId);
                    jobContextService.closeContext(jobId);
                }
            }
        };
    }
}

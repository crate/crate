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

package io.crate.executor.task;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.analyze.AnalyzedStatement;
import io.crate.executor.TaskResult;
import io.crate.planner.node.ddl.GenericDDLNode;
import io.crate.executor.RowCountResult;
import io.crate.executor.JobTask;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;

public class DDLTask extends JobTask {

    private final AnalyzedStatement analyzedStatement;
    private DDLStatementDispatcher ddlStatementDispatcher;
    private SettableFuture<TaskResult> result = SettableFuture.create();
    private List<ListenableFuture<TaskResult>> results = ImmutableList.<ListenableFuture<TaskResult>>of(result);

    public DDLTask(UUID jobId, DDLStatementDispatcher ddlStatementDispatcher, GenericDDLNode genericDDLNode) {
        super(jobId);
        this.ddlStatementDispatcher = ddlStatementDispatcher;
        analyzedStatement = genericDDLNode.analyzedStatement();
    }

    @Override
    public void start() {
        ListenableFuture<Long> future = ddlStatementDispatcher.process(analyzedStatement, jobId());
        Futures.addCallback(future, new FutureCallback<Long>() {
            @Override
            public void onSuccess(Long rowCount) {
                if (rowCount == null) {
                    result.set(TaskResult.ROW_COUNT_UNKNOWN);
                } else {
                    result.set(new RowCountResult(rowCount));
                }
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("DDLTask doesn't support upstreamResults");
    }
}

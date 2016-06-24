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

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.action.sql.ResultReceiver;
import io.crate.analyze.AnalyzedStatement;
import io.crate.core.collections.Row;
import io.crate.core.collections.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.OneRowActionListener;

import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

public class DDLTask extends JobTask {

    private final AnalyzedStatement analyzedStatement;
    private DDLStatementDispatcher ddlStatementDispatcher;

    public DDLTask(UUID jobId, DDLStatementDispatcher ddlStatementDispatcher, AnalyzedStatement analyzedStatement) {
        super(jobId);
        this.ddlStatementDispatcher = ddlStatementDispatcher;
        this.analyzedStatement = analyzedStatement;
    }

    @Override
    public void execute(final ResultReceiver resultReceiver) {
        ListenableFuture<Long> future = ddlStatementDispatcher.dispatch(analyzedStatement, jobId());
        Futures.addCallback(future, new OneRowActionListener<>(resultReceiver, new Function<Long, Row>() {
            @Nullable
            @Override
            public Row apply(@Nullable Long input) {
                return new Row1(input == null ? -1 : input);
            }
        }));
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> executeBulk() {
        throw new UnsupportedOperationException("DDL task cannot be executed as bulk operation");
    }
}

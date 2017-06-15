/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.task;

import io.crate.action.sql.DCLStatementDispatcher;
import io.crate.analyze.AnalyzedStatement;
import io.crate.data.BatchConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class DCLTask extends JobTask {

    private final AnalyzedStatement analyzedStatement;
    private DCLStatementDispatcher dclStatementDispatcher;

    public DCLTask(UUID jobId, DCLStatementDispatcher dclStatementDispatcher, AnalyzedStatement analyzedStatement) {
        super(jobId);
        this.dclStatementDispatcher = dclStatementDispatcher;
        this.analyzedStatement = analyzedStatement;
    }

    @Override
    public void execute(final BatchConsumer consumer, Row parameters) {
        CompletableFuture<Long> future = dclStatementDispatcher.dispatch(analyzedStatement, parameters);

        OneRowActionListener<Long> responseOneRowActionListener = new OneRowActionListener<>(consumer, new Function<Long, Row>() {
            @Nullable
            @Override
            public Row apply(@Nullable Long input) {
                assert input != null : "Row count should not be null, but -1 if unknown";
                return new Row1(input);
            }
        });
        future.whenComplete(responseOneRowActionListener);
    }
}

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

import io.crate.analyze.AnalyzedStatement;
import io.crate.data.RowConsumer;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.JobTask;
import io.crate.executor.transport.OneRowActionListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Generic task that delegates the work to {@link #function} on execute;
 */
public class FunctionDispatchTask extends JobTask {


    private final BiFunction<? super AnalyzedStatement, ? super Row, CompletableFuture<Long>> function;
    private final AnalyzedStatement statement;

    public FunctionDispatchTask(UUID jobId,
                                BiFunction<? super AnalyzedStatement, ? super Row, CompletableFuture<Long>> function,
                                AnalyzedStatement statement) {
        super(jobId);
        this.function = function;
        this.statement = statement;
    }

    @Override
    public void execute(final RowConsumer consumer, Row parameters) {
        CompletableFuture<Long> rowCount = function.apply(statement, parameters);
        rowCount.whenComplete(new OneRowActionListener<>(consumer, rCount -> new Row1(rCount == null ? -1 : rCount)));
    }
}

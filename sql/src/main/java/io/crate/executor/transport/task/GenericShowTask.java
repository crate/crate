/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.task;

import io.crate.action.sql.ShowStatementDispatcher;
import io.crate.analyze.AbstractShowAnalyzedStatement;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.executor.Task;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class GenericShowTask implements Task {

    private final UUID jobId;
    private final ShowStatementDispatcher showStatementDispatcher;
    private final AbstractShowAnalyzedStatement statement;

    public GenericShowTask(UUID jobId, ShowStatementDispatcher showStatementDispatcher, AbstractShowAnalyzedStatement statement) {
        this.jobId = jobId;
        this.showStatementDispatcher = showStatementDispatcher;
        this.statement = statement;
    }

    @Override
    public void execute(RowReceiver rowReceiver, Row parameters) {
        try {
            rowReceiver.setNextRow(new Row1(showStatementDispatcher.process(statement, jobId)));
            rowReceiver.finish(RepeatHandle.UNSUPPORTED);
        } catch (Throwable t) {
            rowReceiver.fail(t);
        }
    }

    @Override
    public List<CompletableFuture<Long>> executeBulk() {
        throw new UnsupportedOperationException("show task cannot be executed as bulk operation");
    }
}

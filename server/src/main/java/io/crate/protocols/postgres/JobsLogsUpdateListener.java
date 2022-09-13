/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.protocols.postgres;

import java.util.UUID;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import io.crate.exceptions.SQLExceptions;
import io.crate.execution.engine.collect.stats.JobsLogs;

public class JobsLogsUpdateListener {

    private final UUID jobId;
    private final JobsLogs jobsLogs;
    private final Consumer<String> onStmtUpdate;
    private final Consumer<Throwable> onExecutionComplete;

    public JobsLogsUpdateListener(UUID jobId, JobsLogs jobsLogs) {
        this.jobId = jobId;
        this.jobsLogs = jobsLogs;
        this.onStmtUpdate = new StmtUpdateListener();
        this.onExecutionComplete = new ExecutionEndListener();
    }

    public Consumer<Throwable> executionEndListener() {
        return onExecutionComplete;
    }

    public Consumer<String> stmtUpdateListener() {
        return onStmtUpdate;
    }

    private class ExecutionEndListener implements Consumer<Throwable> {

        private ExecutionEndListener() {

        }

        @Override
        public void accept(@Nullable Throwable throwable) {
            if (throwable == null) {
                jobsLogs.logExecutionEnd(jobId, null);
            } else {
                jobsLogs.logExecutionEnd(jobId, SQLExceptions.messageOf(throwable));
            }
        }
    }

    private class StmtUpdateListener implements Consumer<String> {

        private StmtUpdateListener() {

        }

        @Override
        public void accept(String newStmt) {
            jobsLogs.logJobContextStatementUpdate(jobId, newStmt);
        }
    }
}

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

package io.crate.action.sql;

import io.crate.data.Row;
import io.crate.operation.projectors.*;

import java.util.Set;

public class RowReceiverToResultReceiver implements RowReceiver {

    private ResultReceiver resultReceiver;
    private int maxRows;
    private long rowCount = 0;
    private volatile boolean interrupted = false;

    private ResumeHandle resumeHandle = null;

    public RowReceiverToResultReceiver(ResultReceiver resultReceiver, int maxRows) {
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    @Override
    public Result setNextRow(Row row) {
        if (interrupted) {
            return Result.STOP;
        }

        rowCount++;
        resultReceiver.setNextRow(row);

        if (maxRows > 0 && rowCount % maxRows == 0) {
            return Result.PAUSE;
        }
        return Result.CONTINUE;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeHandle) {
        this.resumeHandle = resumeHandle;
        resultReceiver.batchFinished();
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        resultReceiver.allFinished(interrupted);
    }

    @Override
    public void fail(Throwable throwable) {
        resultReceiver.fail(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        fail(throwable);
    }

    public void interruptIfResumable() {
        if (!interrupted && resumeHandle != null) {
            interrupted = true;
            resumeHandle.resume(false);
        }
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    public ResumeHandle resumeHandle() {
        return resumeHandle;
    }

    public void replaceResultReceiver(ResultReceiver resultReceiver, int maxRows) {
        this.resumeHandle = null;
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }
}

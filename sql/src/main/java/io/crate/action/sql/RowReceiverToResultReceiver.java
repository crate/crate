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

import io.crate.core.collections.Row;
import io.crate.operation.projectors.*;

import java.util.Set;

class RowReceiverToResultReceiver implements RowReceiver {

    private ResultReceiver resultReceiver;
    private int maxRows;
    private long rowCount = 0;

    private ResumeHandle resumeHandle = null;
    private RowReceiver.Result nextRowResult = Result.CONTINUE;

    RowReceiverToResultReceiver(ResultReceiver resultReceiver, int maxRows) {
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }

    @Override
    public Result setNextRow(Row row) {
        rowCount++;
        resultReceiver.setNextRow(row);

        if (maxRows > 0 && rowCount % maxRows == 0) {
            return Result.PAUSE;
        }
        return nextRowResult;
    }

    @Override
    public void pauseProcessed(ResumeHandle resumeHandle) {
        this.resumeHandle = resumeHandle;
        resultReceiver.batchFinished();
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        resultReceiver.allFinished();
    }

    @Override
    public void fail(Throwable throwable) {
        nextRowResult = Result.STOP;
        resultReceiver.fail(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        fail(throwable);
    }

    @Override
    public void prepare() {
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    ResumeHandle resumeHandle() {
        return resumeHandle;
    }

    void replaceResultReceiver(ResultReceiver resultReceiver, int maxRows) {
        this.resumeHandle = null;
        this.resultReceiver = resultReceiver;
        this.maxRows = maxRows;
    }
}

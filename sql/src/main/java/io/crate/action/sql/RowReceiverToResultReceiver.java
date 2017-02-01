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

public abstract class RowReceiverToResultReceiver implements RowReceiver {

    protected ResultReceiver resultReceiver;

    public static RowReceiverToResultReceiver singleRow(ResultReceiver receiver) {
        return new SingleRow(receiver);
    }

    public static RowReceiverToResultReceiver multiRow(ResultReceiver receiver, int batchSize) {
        return new MultiRow(receiver, batchSize);
    }

    private RowReceiverToResultReceiver(ResultReceiver resultReceiver) {
        this.resultReceiver = resultReceiver;
    }

    @Override
    public void finish(RepeatHandle repeatable) {
        resultReceiver.allFinished();
    }

    @Override
    public void fail(Throwable throwable) {
        resultReceiver.fail(throwable);
    }

    @Override
    public void kill(Throwable throwable) {
        fail(throwable);
    }

    @Override
    public Set<Requirement> requirements() {
        return Requirements.NO_REQUIREMENTS;
    }

    public abstract ResumeHandle resumeHandle();

    public abstract void replaceResultReceiver(ResultReceiver resultReceiver, int batchSize);

    static class SingleRow extends RowReceiverToResultReceiver {

        public SingleRow(ResultReceiver resultReceiver) {
            super(resultReceiver);
        }

        @Override
        public Result setNextRow(Row row) {
            resultReceiver.setNextRow(row);
            return Result.STOP;
        }

        @Override
        public void pauseProcessed(ResumeHandle resumeable) {
            throw new UnsupportedOperationException("pause not supported");
        }

        public ResumeHandle resumeHandle() {
            return null;
        }

        @Override
        public void replaceResultReceiver(ResultReceiver resultReceiver, int batchSize) {
            throw new UnsupportedOperationException("method replaceResultReceiver not suppported");
        }
    }

    static class MultiRow extends RowReceiverToResultReceiver {

        private int maxRows;
        private long rowCount = 0;

        private ResumeHandle resumeHandle = null;

        public MultiRow(ResultReceiver resultReceiver, int maxRows) {
            super(resultReceiver);
            this.maxRows = maxRows;
        }

        @Override
        public ResumeHandle resumeHandle() {
            return resumeHandle;
        }

        @Override
        public void replaceResultReceiver(ResultReceiver resultReceiver, int batchSize) {
            this.resumeHandle = null;
            this.resultReceiver = resultReceiver;
            this.maxRows = batchSize;
        }

        @Override
        public Result setNextRow(Row row) {
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
    }
}

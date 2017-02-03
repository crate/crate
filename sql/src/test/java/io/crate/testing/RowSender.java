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

package io.crate.testing;

import com.google.common.util.concurrent.MoreExecutors;
import io.crate.data.Row;
import io.crate.operation.projectors.ExecutorResumeHandle;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Executor;

public class RowSender implements Runnable, RepeatHandle {

    protected final RowReceiver downstream;
    private final Iterable<Row> rows;
    private final ExecutorResumeHandle resumeable;

    private volatile int numPauses = 0;
    private volatile int numResumes = 0;
    private Iterator<Row> iterator;

    public static RowSender withFailure(RowReceiver rowReceiver, Executor executor) {
        return new FailingSender(rowReceiver, executor);
    }

    public RowSender(final Iterable<Row> rows, RowReceiver rowReceiver, Executor executor) {
        this.rows = rows;
        downstream = rowReceiver;
        iterator = rows.iterator();
        this.resumeable = new ExecutorResumeHandle(executor, new Runnable() {
            @Override
            public void run() {
                numResumes++;
                RowSender.this.run();
            }
        });
    }

    @Override
    public void run() {
        try {
            loop:
            while (iterator.hasNext()) {
                RowReceiver.Result result = downstream.setNextRow(iterator.next());
                switch (result) {
                    case CONTINUE:
                        continue;
                    case PAUSE:
                        numPauses++;
                        downstream.pauseProcessed(resumeable);
                        return;
                    case STOP:
                        break loop;
                }
                throw new AssertionError("Unrecognized setNextRow result: " + result);
            }
        } catch (Throwable t) {
            downstream.fail(t);
            return;
        }
        downstream.finish(this);
    }

    @Override
    public void repeat() {
        iterator = rows.iterator();
        RowSender.this.run();
    }

    public int numPauses() {
        return numPauses;
    }

    public int numResumes() {
        return numResumes;
    }


    /**
     * Generates N rows where each row will just have 1 integer column, the current range iteration value.
     * N is defined by the given <p>start</p> and <p>end</p> arguments.
     *
     * @param start       range start for generating rows (inclusive)
     * @param end         range end for generating rows (exclusive)
     * @param rowReceiver rows will be emitted on that RowReceiver
     * @return the last emitted integer value
     */
    public static long generateRowsInRangeAndEmit(int start, int end, RowReceiver rowReceiver) {
        RowSender rowSender = new RowSender(RowGenerator.range(start, end), rowReceiver, MoreExecutors.directExecutor());
        rowSender.run();
        if (rowSender.iterator.hasNext()) {
            long nextValue = (long) rowSender.iterator.next().get(0);
            return start > end ? nextValue + 1L : nextValue - 1L;
        } else {
            return end;
        }
    }

    private static class FailingSender extends RowSender {

        private FailingSender(RowReceiver rowReceiver, Executor executor) {
            super(Collections.emptyList(), rowReceiver, executor);
        }

        @Override
        public void run() {
            downstream.fail(new IllegalStateException("dummy"));
        }
    }
}

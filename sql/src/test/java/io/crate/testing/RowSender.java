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
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.projectors.ExecutorResumeHandle;
import io.crate.operation.projectors.RepeatHandle;
import io.crate.operation.projectors.RowReceiver;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

public class RowSender implements Runnable, RepeatHandle {

    private final Iterable<Row> rows;
    private final RowReceiver downstream;
    private final ExecutorResumeHandle resumeable;

    private volatile int numPauses = 0;
    private volatile int numResumes = 0;
    private Iterator<Row> iterator;

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
        loop:
        while (iterator.hasNext()) {
            RowReceiver.Result result = downstream.setNextRow(iterator.next());
            switch (result) {
                case CONTINUE:
                    continue ;
                case PAUSE:
                    numPauses++;
                    downstream.pauseProcessed(resumeable);
                    return;
                case STOP:
                    break loop;
            }
            throw new AssertionError("Unrecognized setNextRow result: " + result);
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
     * Generate a range of rows.
     * Both increasing (e.g. 0 -> 10) and decreasing ranges (10 -> 0) are supported.
     *
     * @param from start (inclusive)
     * @param to end (exclusive)
     */
    public static Iterable<Row> rowRange(final long from, final long to) {
        return new Iterable<Row>() {

            @Override
            public Iterator<Row> iterator() {
                return new Iterator<Row>() {

                    private Object[] columns = new Object[1];
                    private RowN sharedRow = new RowN(columns);
                    private long i = from;
                    private long step = from < to ? 1 : -1;

                    @Override
                    public boolean hasNext() {
                        return step >= 0 ? i < to : i > to;
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException("Iterator exhausted");
                        }
                        columns[0] = i;
                        i += step;
                        return sharedRow;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Remove not supported");
                    }
                };
            }
        };
    }

    /**
     * Generates N rows where each row will just have 1 integer column, the current range iteration value.
     * N is defined by the given <p>start</p> and <p>end</p> arguments.
     *
     * @param start         range start for generating rows (inclusive)
     * @param end           range end for generating rows (exclusive)
     * @param rowReceiver   rows will be emitted on that RowReceiver
     * @return              the last emitted integer value
     */
    public static long generateRowsInRangeAndEmit(int start, int end, RowReceiver rowReceiver) {
        RowSender rowSender = new RowSender(rowRange(start, end), rowReceiver, MoreExecutors.directExecutor());
        rowSender.run();
        if (rowSender.iterator.hasNext()) {
            long nextValue = (long) rowSender.iterator.next().get(0);
            return start > end ? nextValue + 1L : nextValue - 1L;
        } else {
            return end;
        }
    }
}

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

package io.crate.operation.projectors;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.data.Row;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

public class IterableRowEmitter implements Runnable, RepeatHandle {

    private final RowReceiver rowReceiver;
    private final Iterable<? extends Row> rows;
    private final ExecutorResumeHandle resumeable;
    private Iterator<? extends Row> rowsIt;

    private final AtomicBoolean finished = new AtomicBoolean(false);

    public IterableRowEmitter(RowReceiver rowReceiver,
                              final Iterable<? extends Row> rows,
                              Optional<Executor> executor) {
        this.rowReceiver = rowReceiver;
        this.rows = rows;
        this.rowsIt = rows.iterator();
        this.resumeable = new ExecutorResumeHandle(executor.or(MoreExecutors.directExecutor()), this);
    }

    public IterableRowEmitter(RowReceiver rowReceiver, Iterable<? extends Row> rows) {
        this(rowReceiver, rows, Optional.<Executor>absent());
    }

    public void kill(Throwable t) {
        rowReceiver.kill(t);
    }

    @Override
    public void run() {
        try {
            loop:
            while (rowsIt.hasNext()) {
                RowReceiver.Result result = rowReceiver.setNextRow(rowsIt.next());
                switch (result) {
                    case CONTINUE:
                        continue;
                    case PAUSE:
                        rowReceiver.pauseProcessed(resumeable);
                        return;
                    case STOP:
                        break loop;
                }
                throw new AssertionError("Unrecognized setNextRow result: " + result);
            }
            if (finished.compareAndSet(false, true)) {
                rowReceiver.finish(this);
            } else {
                throw new IllegalStateException("Illegal finished state. Should have been false but was true");
            }
        } catch (Throwable t) {
            rowReceiver.fail(t);
        }
    }

    @Override
    public void repeat() {
        if (finished.compareAndSet(true, false)) {
            rowsIt = rows.iterator();
            IterableRowEmitter.this.run();
        } else {
            throw new IllegalStateException("Downstream called repeat, but IterableRowEmitter wasn't finished");
        }
    }
}

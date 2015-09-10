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
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class IterableRowEmitter implements RowUpstream, Runnable {

    private final static ESLogger LOGGER = Loggers.getLogger(IterableRowEmitter.class);

    private final RowReceiver rowReceiver;
    private final ExecutionState executionState;
    private final Iterable<? extends Row> rows;
    private Iterator<? extends Row> rowsIt;
    private final Executor executor;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    private volatile boolean pendingPause = false;

    public IterableRowEmitter(RowReceiver rowReceiver,
                              ExecutionState executionState,
                              Iterable<? extends Row> rows,
                              Optional<Executor> executor) {
        this.rowReceiver = rowReceiver;
        this.executionState = executionState;
        rowReceiver.setUpstream(this);
        this.rows = rows;
        this.rowsIt = rows.iterator();
        this.executor = executor.or(MoreExecutors.directExecutor());
    }

    public IterableRowEmitter(RowReceiver rowReceiver, ExecutionState executionState, Iterable<? extends Row> rows) {
        this(rowReceiver, executionState, rows, Optional.<Executor>absent());
    }

    @Override
    public void run() {
        try {
            while (rowsIt.hasNext()) {
                if (executionState != null && executionState.isKilled()) {
                    rowReceiver.fail(new CancellationException());
                    return;
                }
                boolean wantsMore = rowReceiver.setNextRow(rowsIt.next());
                if (!wantsMore) {
                    break;
                }
                if (processPause()) return;
            }
            rowReceiver.finish();
        } catch (Throwable t) {
            rowReceiver.fail(t);
        }
    }

    private boolean processPause() {
        if (pendingPause) {
            paused.set(true);
            pendingPause = false;
            return true;
        }
        return false;
    }

    @Override
    public void pause() {
        pendingPause = true;
    }

    @Override
    public void resume(boolean async) {
        LOGGER.trace("Received resume from: " + rowReceiver);
        pendingPause = false;
        if (paused.compareAndSet(true, false)) {
            if (async) {
                try {
                    executor.execute(this);
                } catch (EsRejectedExecutionException | RejectedExecutionException e) {
                    run();
                }
            } else {
                run();
            }
        }
    }

    @Override
    public void repeat() {
        rowsIt = rows.iterator();
        run();
    }
}

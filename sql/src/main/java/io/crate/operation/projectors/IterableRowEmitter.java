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
import io.crate.operation.collect.collectors.TopRowUpstream;

import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;

public class IterableRowEmitter implements Runnable {

    private final RowReceiver rowReceiver;
    private final ExecutionState executionState;
    private final TopRowUpstream topRowUpstream;

    private Iterator<? extends Row> rowsIt;

    public IterableRowEmitter(RowReceiver rowReceiver,
                              ExecutionState executionState,
                              Iterator<? extends Row> rows) {
        this.rowReceiver = rowReceiver;
        this.executionState = executionState;
        topRowUpstream = new TopRowUpstream(MoreExecutors.directExecutor(), this);
        rowReceiver.setUpstream(topRowUpstream);
        this.rowsIt = rows;
    }

    public IterableRowEmitter(RowReceiver rowReceiver,
                              ExecutionState executionState,
                              Iterable<? extends Row> rows,
                              Optional<Executor> executor) {
        this.rowReceiver = rowReceiver;
        this.executionState = executionState;
        topRowUpstream = new TopRowUpstream(executor.or(MoreExecutors.directExecutor()), this);
        rowReceiver.setUpstream(topRowUpstream);
        this.rowsIt = rows.iterator();
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
                if (topRowUpstream.shouldPause()) {
                    topRowUpstream.pauseProcessed();
                    return;
                }
            }
            rowReceiver.finish();
        } catch (Throwable t) {
            rowReceiver.fail(t);
        }
    }
}

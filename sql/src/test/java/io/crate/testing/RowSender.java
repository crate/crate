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

import io.crate.core.collections.Row;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.RowReceiver;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RowSender implements Runnable, RowUpstream {

    private final RowReceiver downstream;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final Executor executor;
    private volatile boolean pendingPause = false;

    private volatile int numPauses = 0;
    private volatile int numResumes = 0;
    private Iterator<Row> iterator;

    public RowSender(Iterable<Row> rows, RowReceiver rowReceiver, Executor executor) {
        this.executor = executor;
        downstream = rowReceiver;
        rowReceiver.setUpstream(this);
        iterator = rows.iterator();
    }

    @Override
    public void run() {
        while (iterator.hasNext()) {
            final boolean wantsMore = downstream.setNextRow(iterator.next());
            if (!wantsMore) {
                break;
            }
            if (processPause()) return;
        }
        downstream.finish();
   }

    private boolean processPause() {
        if (pendingPause) {
            numPauses++;
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
        pendingPause = false;
        if (paused.compareAndSet(true, false)) {
            numResumes++;
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

    /**
     * tells the RowUpstream that it should push all rows again
     */
    @Override
    public void repeat() {
        throw new UnsupportedOperationException();
    }

    public int numPauses() {
        return numPauses;
    }

    public int numResumes() {
        return numResumes;
    }
}

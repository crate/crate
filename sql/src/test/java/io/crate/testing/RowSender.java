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
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.operation.RowUpstream;
import io.crate.operation.collect.collectors.TopRowUpstream;
import io.crate.operation.projectors.RowReceiver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executor;

public class RowSender implements Runnable, RowUpstream {

    private final RowReceiver downstream;
    private final TopRowUpstream topRowUpstream;

    private volatile int numPauses = 0;
    private volatile int numResumes = 0;
    private Iterator<Row> iterator;

    public RowSender(final Iterable<Row> rows, RowReceiver rowReceiver, Executor executor) {
        downstream = rowReceiver;
        topRowUpstream = new TopRowUpstream(executor, new Runnable() {
            @Override
            public void run() {
                numResumes++;
                RowSender.this.run();
            }
        }, new Runnable() {
            @Override
            public void run() {
                iterator = rows.iterator();
                RowSender.this.run();
            }
        });
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
        if (topRowUpstream.shouldPause()) {
            numPauses++;
            topRowUpstream.pauseProcessed();
            return true;
        }
        return false;
    }

    @Override
    public void pause() {
        topRowUpstream.pause();
    }

    @Override
    public void resume(boolean async) {
        topRowUpstream.resume(async);
    }

    @Override
    public void repeat() {
        topRowUpstream.repeat();
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
     * @param start         range start for generating rows (inclusive)
     * @param end           range end for generating rows (exclusive)
     * @param reverse       if true, list of rows will be reversed before emitting
     * @param rowReceiver   rows will be emitted on that RowReceiver
     * @return              the last emitted integer value
     */
    public static int generateRowsInRangeAndEmit(int start, int end, boolean reverse, RowReceiver rowReceiver) {
        final List<Object[]> rows = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            rows.add(new Object[]{i});
        }
        if (reverse) {
            Collections.reverse(rows);
        }

        RowSender rowSender = new RowSender(new CollectionBucket(rows), rowReceiver, MoreExecutors.directExecutor());
        rowSender.run();
        if (rowSender.iterator.hasNext()) {
            Integer nextValue = (Integer) rowSender.iterator.next().get(0);
            return reverse ? nextValue + 1 : nextValue - 1;
        } else {
            return end;
        }
    }
}

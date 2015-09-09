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

import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ForwardingRowDownstream implements RowDownstream, RowUpstream {

    private static final ESLogger LOGGER = Loggers.getLogger(ForwardingRowDownstream.class);

    private final Set<RowUpstream> rowUpstreams = new HashSet<>();
    private final AtomicInteger activeUpstreams = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final RowReceiver rowReceiver;


    public ForwardingRowDownstream(final RowReceiver rowReceiver) {
        rowReceiver.setUpstream(this);
        this.rowReceiver = new MultiUpstreamRowReceiver(rowReceiver);
    }

    @Override
    public RowReceiver newRowReceiver() {
        activeUpstreams.incrementAndGet();
        return rowReceiver;
    }

    @Override
    public void pause() {
        for (RowUpstream rowUpstream : rowUpstreams) {
            rowUpstream.pause();
        }
    }

    @Override
    public void resume(boolean async) {
        for (RowUpstream rowUpstream : rowUpstreams) {
            rowUpstream.resume(async);
        }
    }

    class MultiUpstreamRowReceiver implements RowReceiver {

        final RowReceiver rowReceiver;
        private AtomicBoolean prepared = new AtomicBoolean(false);

        public MultiUpstreamRowReceiver(RowReceiver rowReceiver) {
            this.rowReceiver = rowReceiver;
        }

        @Override
        public boolean setNextRow(Row row) {
            synchronized (rowReceiver) {
                return rowReceiver.setNextRow(row);
            }
        }

        @Override
        public void finish() {
            countdown();
        }

        @Override
        public void fail(Throwable throwable) {
            failure.set(throwable);
            countdown();
        }

        @Override
        public void prepare(ExecutionState executionState) {
            if (prepared.compareAndSet(false, true)) {
                rowReceiver.prepare(executionState);
            }
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            synchronized (rowReceiver) {
                if (!rowUpstreams.add(rowUpstream)) {
                    LOGGER.debug("Upstream {} registered itself twice", rowUpstream);
                }
            }
        }

        private void countdown() {
            if (activeUpstreams.decrementAndGet() == 0) {
                Throwable t = failure.get();
                if (t == null) {
                    rowReceiver.finish();
                } else {
                    rowReceiver.fail(t);
                }
            }
        }
    }
}

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

import com.google.common.annotations.VisibleForTesting;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ForwardingProjector implements Projector {

    @VisibleForTesting
    final RowPipe pipe;

    private final List<RowUpstream> upstreams = new ArrayList<>();
    private final AtomicInteger activeUpstreams = new AtomicInteger();
    private final RowDownstreamHandle handle;
    private boolean downstreamRegistered = false;

    public ForwardingProjector(final RowPipe pipe) {
        this.pipe = pipe;
        this.handle = new RowDownstreamHandle() {

            private final AtomicReference<Throwable> failure = new AtomicReference<>();

            @Override
            public synchronized boolean setNextRow(Row row) {
                return pipe.setNextRow(row);
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

            private void countdown() {
                if (activeUpstreams.decrementAndGet() == 0) {
                    propagateFinish();
                }
            }

            private void propagateFinish() {
                Throwable throwable = failure.get();
                if (throwable == null) {
                    pipe.finish();
                } else {
                    pipe.fail(throwable);
                }
            }
        };
    }

    @Override
    public void startProjection(ExecutionState executionState) {

    }

    @Override
    public void downstream(RowDownstream downstream) {
        Preconditions.checkState(!downstreamRegistered, "ForwardingProjector supports only 1 downstream");
        pipe.downstream(downstream.registerUpstream(this));
        downstreamRegistered = true;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        activeUpstreams.incrementAndGet();
        upstreams.add(upstream);
        return handle;
    }

    @Override
    public void pause() {
        for (RowUpstream upstream : upstreams) {
            upstream.pause();
        }
    }

    @Override
    public void resume(boolean async) {
        for (RowUpstream upstream : upstreams) {
            upstream.resume(async);
        }
    }
}

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

package io.crate.operation.projectors;

import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class RowDownstreamAndHandle implements RowDownstream, RowUpstream, RowDownstreamHandle {

    private final List<RowUpstream> upstreams = new ArrayList<>(1);
    protected final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean failed = new AtomicBoolean(false);

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        upstreams.add(upstream);
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() == 0 && !failed.get()) {
            onAllUpstreamsFinished();
        }
    }

    protected void onAllUpstreamsFinished() {

    }

    @Override
    public void fail(Throwable throwable) {
        failed.set(true);
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
            upstream.resume(false);
        }
    }


    public void repeat() {
        for (RowUpstream upstream : upstreams) {
            upstream.repeat();
        }
    }
}

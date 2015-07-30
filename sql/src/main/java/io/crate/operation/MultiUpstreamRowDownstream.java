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

package io.crate.operation;

import io.crate.core.collections.Row;
import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MultiUpstreamRowDownstream implements RowDownstream, RowDownstreamHandle {

    private final AtomicInteger pendingUpstreams = new AtomicInteger(0);
    private final List<RowUpstream> registeredUpstreams = new ArrayList<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @Nullable
    private RowDownstreamHandle downstreamHandle;
    private RowDownstreamHandle downstreamHandleProxy = new RowDownstreamHandle() {
        @Override
        public void finish() {
            if (downstreamHandle != null) {
                downstreamHandle.finish();
            }
        }

        @Override
        public void fail(Throwable t) {
            if (downstreamHandle != null) {
                downstreamHandle.fail(t);
            }
        }

        @Override
        public boolean setNextRow(Row row) {
            return downstreamHandle == null || downstreamHandle.setNextRow(row);
        }
    };

    public void downstreamHandleProxy(RowDownstreamHandle downstreamHandleProxy) {
        this.downstreamHandleProxy = downstreamHandleProxy;
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        pendingUpstreams.incrementAndGet();
        registeredUpstreams.add(upstream);
        return this;
    }

    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "SimplifiableIfStatement"})
    @Override
    public boolean setNextRow(Row row) {
        if (failure.get() != null) {
            return false;
        }
        return downstreamHandleProxy.setNextRow(row);
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void finish() {
        int upstreams = pendingUpstreams.decrementAndGet();
        assert upstreams >= 0 : "upstreams may not get negative";
        if (upstreams == 0 && failure.get() == null) {
            downstreamHandleProxy.finish();
        }
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    @Override
    public void fail(Throwable throwable) {
        int upstreams = pendingUpstreams.decrementAndGet();
        assert upstreams >= 0 : "upstreams may not get negative";
        if (failure.compareAndSet(null, throwable)) {
            downstreamHandleProxy.fail(throwable);
        }
    }

    public void downstreamHandle(RowDownstreamHandle downstreamHandle) {
        this.downstreamHandle = downstreamHandle;
    }

    @Nullable
    public RowDownstreamHandle downstreamHandle() {
        return downstreamHandle;
    }

    public List<RowUpstream> upstreams() {
        return registeredUpstreams;
    }

    public int pendingUpstreams() {
        return pendingUpstreams.get();
    }

    public boolean upstreamsRunning() {
        return pendingUpstreams.get() > 0;
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public boolean allUpstreamsFinishedSuccessful() {
        return pendingUpstreams.get() == 0 && failure.get() == null;
    }

    public Throwable failure() {
        return failure.get();
    }
}
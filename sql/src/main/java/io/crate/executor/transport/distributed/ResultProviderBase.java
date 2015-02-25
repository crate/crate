/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport.distributed;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.core.collections.Bucket;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.projectors.ResultProvider;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ResultProviderBase implements ResultProvider {

    protected final SettableFuture<Bucket> result = SettableFuture.create();
    protected final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> lastException = new AtomicReference<>();

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void startProjection() {
        if (remainingUpstreams.get() <= 0) {
            finishProjection();
        }
    }

    public abstract void finishProjection();

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() <= 0) {
            if (lastException.get() != null) {
                result.setException(lastException.get());
            } else {
                finishProjection();
            }
        }
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        lastException.set(throwable);
        upstreamFinished();
    }

    @Override
    public ListenableFuture<Bucket> result() {
        return result;
    }
}

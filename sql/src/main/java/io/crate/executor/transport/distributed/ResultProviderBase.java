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
import io.crate.jobs.ExecutionState;
import io.crate.operation.*;
import io.crate.operation.projectors.ResultProvider;

public abstract class ResultProviderBase implements ResultProvider {

    private final SettableFuture<Bucket> result = SettableFuture.create();
    protected final MultiUpstreamRowDownstream multiUpstreamRowDownstream = new MultiUpstreamRowDownstream();
    private final MultiUpstreamRowUpstream multiUpstreamRowUpstream = new MultiUpstreamRowUpstream(multiUpstreamRowDownstream);

    protected ExecutionState executionState;

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        multiUpstreamRowDownstream.registerUpstream(upstream);
        return this;
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    /**
     * Do the things necessary to finish this projection and produce
     * the resulting Bucket.
     *
     * @return a Bucket to be set on the result.
     */
    protected abstract Bucket doFinish();

    /**
     * Do the cleanup necessary on failure.
     * And properly react to the exception and/or transform it if necessary.
     *
     * @param t the exception caused the upstream to fail
     * @return a Throwable to be set on the result
     */
    protected abstract Throwable doFail(Throwable t);

    @Override
    public void finish() {
        multiUpstreamRowDownstream.finish();
        if (multiUpstreamRowDownstream.allUpstreamsFinishedSuccessful()) {
            result.set(doFinish());
        }
    }

    @Override
    public void fail(Throwable throwable) {
        multiUpstreamRowDownstream.fail(throwable);
        result.setException(doFail(throwable));
    }

    @Override
    public ListenableFuture<Bucket> result() {
        return result;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        throw new UnsupportedOperationException("Setting downstream isn't supported on ResultProvider");
    }

    @Override
    public void pause() {
        multiUpstreamRowUpstream.pause();
    }

    @Override
    public void resume(boolean async) {
        multiUpstreamRowUpstream.resume(async);
    }
}

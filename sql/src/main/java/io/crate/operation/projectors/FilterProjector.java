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

package io.crate.operation.projectors;

import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Function;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This implementation is currently only a stub.
 * TODO: implement functionality
 *
 */
public class FilterProjector implements Projector {

    private final CollectExpression[] collectExpressions;
    private final Function query;

    private Projector downstream;
    private AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    public FilterProjector(CollectExpression[] collectExpressions,
                           Function query) {
        this.collectExpressions = collectExpressions;
        this.query = query;
    }

    @Override
    public void startProjection() {
        for (CollectExpression collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }

        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public boolean setNextRow(Object... row) {
        if (downstream != null) {
            downstream.setNextRow(row);
            Throwable throwable = upstreamFailure.get();
            if (throwable != null) {
                downstream.upstreamFailed(throwable);
            } else {
                downstream.upstreamFinished();
            }
        }
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        remainingUpstreams.decrementAndGet();
    }

    @Override
    public void upstreamFailed(Throwable throwable) {
        upstreamFailure.set(throwable);
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            downstream.upstreamFailed(throwable);
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public Projector downstream() {
        return downstream;
    }
}

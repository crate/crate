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

import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.collect.CollectExpression;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class FilterProjector implements Projector, ProjectorUpstream {

    private final CollectExpression[] collectExpressions;
    private final Input<Boolean> condition;

    private Projector downstream;
    private AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    public FilterProjector(CollectExpression[] collectExpressions,
                           Input<Boolean> condition) {
        this.collectExpressions = collectExpressions;
        this.condition = condition;
    }

    @Override
    public void startProjection() {
        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }

        Boolean queryResult = condition.value();
        if (queryResult == null) {
            return true;
        }

        if (downstream != null && queryResult) {
            return downstream.setNextRow(row);
        }
        return true;
    }

    @Override
    public void registerUpstream(ProjectorUpstream upstream) {
        remainingUpstreams.incrementAndGet();
    }

    @Override
    public void upstreamFinished() {
        if (remainingUpstreams.decrementAndGet() > 0) {
            return;
        }
        if (downstream != null) {
            Throwable throwable = upstreamFailure.get();
            if (throwable == null) {
                downstream.upstreamFinished();
            } else {
                downstream.upstreamFailed(throwable);
            }
        }
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
}

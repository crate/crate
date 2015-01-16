/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.breaker.RamAccountingContext;
import io.crate.operation.AggregationContext;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AggregationProjector implements Projector {

    private final Aggregator[] aggregators;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] row;
    private final Object[] states;
    private Projector downstream;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicReference<Throwable> upstreamFailure = new AtomicReference<>(null);

    public AggregationProjector(Set<CollectExpression<?>> collectExpressions,
                                AggregationContext[] aggregations,
                                RamAccountingContext ramAccountingContext) {
        row = new Object[aggregations.length];
        states = new Object[aggregations.length];
        this.collectExpressions = collectExpressions;
        aggregators = new Aggregator[aggregations.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(
                    ramAccountingContext,
                    aggregations[i].symbol(),
                    aggregations[i].function(),
                    aggregations[i].inputs()
            );
            // prepareState creates the aggregationState. In case of the AggregationProjector
            // we only want to have 1 global state not 1 state per node/shard or even document.
            states[i] = aggregators[i].prepareState();
        }
    }

    @Override
    public void startProjection() {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }

        if (remainingUpstreams.get() <= 0) {
            upstreamFinished();
        }
    }

    @Override
    public void downstream(Projector downstream) {
        this.downstream = downstream;
        downstream.registerUpstream(this);
    }

    @Override
    public synchronized boolean setNextRow(Object... row) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            states[i] = aggregator.processRow(states[i]);

        }
        //noinspection ThrowableResultOfMethodCallIgnored
        return upstreamFailure.get() == null;
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
        for (int i = 0; i < aggregators.length; i++) {
            row[i] = aggregators[i].finishCollect(states[i]);
        }
        if (downstream != null) {
            downstream.setNextRow(row);
            Throwable throwable = upstreamFailure.get();
            if (throwable != null) {
                downstream.upstreamFailed(throwable);
            } else {
                downstream.upstreamFinished();
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
}

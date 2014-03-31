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

import io.crate.operation.AggregationContext;
import io.crate.operation.ProjectorUpstream;
import io.crate.operation.aggregation.AggregationCollector;
import io.crate.operation.collect.CollectExpression;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class AggregationProjector implements Projector {

    private final AggregationCollector[] aggregationCollectors;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] row;
    private Projector downstream;
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);

    public AggregationProjector(Set<CollectExpression<?>> collectExpressions,
                                AggregationContext[] aggregations) {

        row = new Object[aggregations.length];
        this.collectExpressions = collectExpressions;
        aggregationCollectors = new AggregationCollector[aggregations.length];
        for (int i = 0; i < aggregationCollectors.length; i++) {
            aggregationCollectors[i] = new AggregationCollector(
                    aggregations[i].symbol(),
                    aggregations[i].function(),
                    aggregations[i].inputs()
            );
            // startCollect creates the aggregationState. In case of the AggregationProjector
            // we only want to have 1 global state not 1 state per node/shard or even document.
            aggregationCollectors[i].startCollect();
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
    public Projector downstream() {
        return downstream;
    }

    @Override
    public synchronized boolean setNextRow(Object... row) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        for (AggregationCollector aggregationCollector : aggregationCollectors) {
            aggregationCollector.processRow();
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
        for (int i = 0; i < aggregationCollectors.length; i++) {
            row[i] = aggregationCollectors[i].finishCollect();
        }
        if (downstream != null) {
            downstream.setNextRow(row);
            downstream.upstreamFinished();
        }
    }
}

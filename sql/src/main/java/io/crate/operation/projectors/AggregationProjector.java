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
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.distributed.ResultProviderBase;
import io.crate.operation.AggregationContext;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

import java.util.Set;

public class AggregationProjector extends ResultProviderBase implements Projector, RowUpstream {

    private final Aggregator[] aggregators;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] cells;
    private final Row row;
    private final Object[] states;
    private RowDownstreamHandle downstream;

    public AggregationProjector(Set<CollectExpression<?>> collectExpressions,
                                AggregationContext[] aggregations,
                                RamAccountingContext ramAccountingContext) {
        cells = new Object[aggregations.length];
        row = new RowN(cells);
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
        super.startProjection();
    }

    @Override
    public void downstream(RowDownstream downstream) {
        assert this.downstream == null;
        this.downstream = downstream.registerUpstream(this);
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            states[i] = aggregator.processRow(states[i]);
        }
        return true;
    }

    @Override
    public Throwable doFail(Throwable t) {
        if (downstream != null) {
            downstream.fail(t);
        }
        return t;
    }

    @Override
    public Bucket doFinish() {
        for (int i = 0; i < aggregators.length; i++) {
            cells[i] = aggregators[i].finishCollect(states[i]);
        }
        if (downstream != null) {
            downstream.setNextRow(row);
            downstream.finish();
        }
        return new ArrayBucket(new Object[][]{cells});
    }
}

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
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

import java.util.Set;

public class AggregationPipe extends AbstractProjector {

    private final Aggregator[] aggregators;
    private final Set<CollectExpression<Row, ?>> collectExpressions;
    private final Object[] cells;
    private final Row row;
    private final Object[] states;

    public AggregationPipe(Set<CollectExpression<Row, ?>> collectExpressions,
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
    public Result setNextRow(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            states[i] = aggregator.processRow(states[i]);
        }
        return Result.CONTINUE;
    }

    @Override
    public void fail(Throwable t) {
        downstream.fail(t);
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        for (int i = 0; i < aggregators.length; i++) {
            cells[i] = aggregators[i].finishCollect(states[i]);
        }
        try {
            downstream.setNextRow(row);
        } catch (Throwable t) {
            downstream.fail(t);
            return;
        }

        downstream.finish(RepeatHandle.UNSUPPORTED);
    }
}

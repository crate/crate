/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors.ng;

import io.crate.data.*;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

import java.util.Collections;

public class AggregationBIP implements BatchIteratorProjector{

    private final Aggregator[] aggregators;
    private final Iterable<CollectExpression<Row, ?>> expressions;

    public AggregationBIP(Aggregator[] aggregators, Iterable<CollectExpression<Row, ?>> expressions) {
        this.aggregators = aggregators;
        this.expressions = expressions;
    }

    @Override
    public BatchIterator apply(BatchIterator batchIterator) {
        Listener l = new Listener(RowBridging.toRow(batchIterator.rowData()));
        return CollectingProjectors.collecting(batchIterator, l, Columns.of(l.states));
    }

    class Listener implements CollectingProjectors.BatchCollectorListener {

        final Object[] states;
        private final Row row;

        Listener(Row row) {
            this.row = row;
            this.states = new Object[aggregators.length];
        }

        @Override
        public void pre() {
            for (int i = 0; i < aggregators.length; i++) {
                states[i] = aggregators[i].prepareState();
            }
        }

        @Override
        public boolean onRow() {
            for (CollectExpression<Row, ?> collectExpression : expressions) {
                collectExpression.setNextRow(row);
            }
            for (int i = 0; i < aggregators.length; i++) {
                Aggregator aggregator = aggregators[i];
                states[i] = aggregator.processRow(states[i]);
            }
            return true;
        }

        @Override
        public Iterable<?> post() {
            for (int i = 0; i < aggregators.length; i++) {
                states[i] = aggregators[i].finishCollect(states[i]);
            }
            return Collections.singletonList(null);
        }
    }
}

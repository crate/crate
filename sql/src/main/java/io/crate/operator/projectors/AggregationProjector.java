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

package io.crate.operator.projectors;

import io.crate.operator.aggregation.AggregationCollector;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.operations.AggregationContext;

import java.util.Iterator;
import java.util.Set;

public class AggregationProjector implements Projector {

    private static class RowIterator implements Iterator<Object[]> {

        private final Object[] row;
        private boolean hasNext = true;

        public RowIterator(Object[] row) {
            this.row = row;
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Object[] next() {
            hasNext = false;
            return row;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not supported");
        }
    }

    private final AggregationCollector[] aggregationCollectors;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] row;
    private Projector downStream;

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
        }
    }

    @Override
    public void setDownStream(Projector downStream) {
        this.downStream = downStream;
    }

    @Override
    public void startProjection() {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }

        for (AggregationCollector aggregationCollector : aggregationCollectors) {
            aggregationCollector.startCollect();
        }
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
    public void finishProjection() {
        for (int i = 0; i < aggregationCollectors.length; i++) {
            row[i] = aggregationCollectors[i].finishCollect();
        }

        if (downStream != null) {
            downStream.startProjection();
            downStream.setNextRow(row);
            downStream.finishProjection();
        }
    }

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return new Object[][] { row };
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new RowIterator(row);
    }
}

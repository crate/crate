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
import io.crate.operator.operations.ImplementationSymbolVisitor;

import java.util.Set;

public class AggregationProjector implements Projector {

    private final AggregationCollector[] aggregationCollectors;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] row;
    private Projector upstream;

    public AggregationProjector(Set<CollectExpression<?>> collectExpressions,
                                ImplementationSymbolVisitor.AggregationContext[] aggregations) {

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
    public void setUpStream(Projector upStream) {
        this.upstream = upStream;
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

        if (upstream != null) {
            upstream.startProjection();
            upstream.setNextRow(row);
            upstream.finishProjection();
        }
    }

    @Override
    public Object[][] getRows() throws IllegalStateException {
        return new Object[][] { row };
    }
}

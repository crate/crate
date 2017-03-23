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
import io.crate.data.*;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AggregationPipe implements Projector {

    private final AggregateCollector collector;
    private final int numAggregations;

    public AggregationPipe(List<CollectExpression<Row, ?>> expressions,
                           AggregationContext[] aggregations,
                           RamAccountingContext ramAccountingContext) {
        numAggregations = aggregations.length;
        collector = new AggregateCollector(
            expressions,
            ramAccountingContext,
            aggregations[0].symbol().mode(),
            Stream.of(aggregations).map(AggregationContext::function).toArray(AggregationFunction[]::new),
            Stream.of(aggregations).map(AggregationContext::inputs).toArray(Input[][]::new)
        );
    }

    @Override
    public BatchIterator apply(BatchIterator batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator,
            Collectors.collectingAndThen(
                collector,
                cells -> Collections.singletonList(new RowN(cells))),
            numAggregations);
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }
}

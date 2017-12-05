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

import io.crate.analyze.symbol.AggregateMode;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class AggregationPipe implements Projector {

    private final AggregateCollector collector;

    AggregationPipe(List<CollectExpression<Row, ?>> expressions,
                    AggregateMode aggregateMode,
                    AggregationContext[] aggregations,
                    RamAccountingContext ramAccountingContext,
                    Version indexVersionCreated,
                    BigArrays bigArrays) {
        AggregationFunction[] functions = new AggregationFunction[aggregations.length];
        Input[][] inputs = new Input[aggregations.length][];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationContext aggregation = aggregations[i];
            functions[i] = aggregation.function();
            inputs[i] = aggregation.inputs();
        }
        collector = new AggregateCollector(
            expressions,
            ramAccountingContext,
            aggregateMode,
            functions,
            indexVersionCreated,
            bigArrays,
            inputs
        );
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        Collector<Row, ?, Iterable<Row>> collectAndConvertToRows = Collectors.collectingAndThen(
            collector,
            cells -> {
                Row row = new RowN(cells);
                return Collections.singletonList(row);
            });
        return CollectingBatchIterator.newInstance(batchIterator, collectAndConvertToRows);
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }
}

/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccounting;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.memory.MemoryManager;
import org.elasticsearch.Version;

import java.util.List;

public class AggregationPipe implements Projector {

    private final AggregateCollector collector;

    public AggregationPipe(List<CollectExpression<Row, ?>> expressions,
                           AggregateMode aggregateMode,
                           AggregationContext[] aggregations,
                           RamAccounting ramAccounting,
                           MemoryManager memoryManager,
                           Version minNodeVersion,
                           Version indexVersionCreated) {
        AggregationFunction[] functions = new AggregationFunction[aggregations.length];
        Input[][] inputs = new Input[aggregations.length][];
        Input[] filters = new Input[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationContext aggregation = aggregations[i];
            functions[i] = aggregation.function();
            inputs[i] = aggregation.inputs();
            filters[i] = aggregation.filter();
        }

        collector = new AggregateCollector(
            expressions,
            ramAccounting,
            memoryManager,
            minNodeVersion,
            aggregateMode,
            functions,
            indexVersionCreated,
            inputs,
            filters
        );
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator, collector);
    }

    public AggregateCollector getCollector() {
        return collector;
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }
}

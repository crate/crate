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
import io.crate.data.*;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.collect.CollectExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.stream.Stream;

public class GroupingProjector implements Projector {

    private final GroupingCollector<Object> collector;
    private final int numCols;


    public GroupingProjector(List<? extends DataType> keyTypes,
                             List<Input<?>> keyInputs,
                             CollectExpression<Row, ?>[] collectExpressions,
                             AggregationContext[] aggregations,
                             RamAccountingContext ramAccountingContext) {
        assert keyTypes.size() == keyInputs.size() : "number of key types must match with number of key inputs";
        assert allTypesKnown(keyTypes) : "must have a known type for each key input";


        AggregationFunction[] functions = Stream.of(aggregations)
            .map(AggregationContext::function)
            .toArray(AggregationFunction[]::new);
        Input[][] inputs = Stream.of(aggregations)
            .map(AggregationContext::inputs)
            .toArray(Input[][]::new);

        AggregateMode mode = aggregations.length > 0 ? aggregations[0].symbol().mode() : AggregateMode.ITER_FINAL;
        if (keyInputs.size() == 1) {
            collector = GroupingCollector.singleKey(
                collectExpressions,
                mode,
                functions,
                inputs,
                ramAccountingContext,
                keyInputs.get(0),
                keyTypes.get(0)
            );
        } else {
            //noinspection unchecked
            collector = (GroupingCollector<Object>) (GroupingCollector) GroupingCollector.manyKeys(
                collectExpressions,
                mode,
                functions,
                inputs,
                ramAccountingContext,
                keyInputs,
                keyTypes
            );
        }
        numCols = keyInputs.size() + functions.length;
    }

    private static boolean allTypesKnown(List<? extends DataType> keyTypes) {
        return keyTypes.stream().noneMatch(input -> input.equals(DataTypes.UNDEFINED));
    }

    @Override
    public BatchIterator apply(BatchIterator batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator, collector, numCols);
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }
}

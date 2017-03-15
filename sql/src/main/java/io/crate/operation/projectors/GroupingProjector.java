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
import io.crate.data.BatchIteratorProjector;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;

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

        Aggregator[] aggregators = new Aggregator[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            aggregators[i] = new Aggregator(
                ramAccountingContext,
                aggregations[i].symbol(),
                aggregations[i].function(),
                aggregations[i].inputs()
            );
        }
        if (keyInputs.size() == 1) {
            collector = GroupingCollector.singleKey(
                collectExpressions,
                aggregators,
                ramAccountingContext,
                keyInputs.get(0),
                keyTypes.get(0)
            );
        } else {
            //noinspection unchecked
            collector = (GroupingCollector<Object>) (GroupingCollector) GroupingCollector.manyKeys(
                collectExpressions,
                aggregators,
                ramAccountingContext,
                keyInputs,
                keyTypes
            );
        }
        numCols = keyInputs.size() + aggregators.length;
    }

    private static boolean allTypesKnown(List<? extends DataType> keyTypes) {
        return keyTypes.stream().noneMatch(input -> input.equals(DataTypes.UNDEFINED));
    }

    @Override
    public BatchIteratorProjector asProjector() {
        return bi -> CollectingBatchIterator.newInstance(bi, collector, numCols);
    }
}
